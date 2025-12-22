package commitlog

import (
	"context"
	"errors"
	"io"
	"sync"

	pkgErrors "github.com/pkg/errors"
)

// ErrCommitLogReadonly is returned when the end of a readonly CommitLog has
// been reached.
var ErrCommitLogReadonly = errors.New("end of readonly log")

// MessageReader is the interface implemented by both Reader and ReverseReader.
// It allows reading messages from a CommitLog either forwards or backwards.
type MessageReader interface {
	ReadMessage(ctx context.Context, headersBuf []byte) (SerializedMessage, int64, int64, uint64, error)
}

type contextReader interface {
	Read(context.Context, []byte) (int, error)
}

// Reader reads messages atomically from a CommitLog. Readers should not be
// used concurrently.
type Reader struct {
	ctxReader   contextReader
	offset      int64
	log         *commitLog
	uncommitted bool
}

// NewReader creates a new Reader starting at the given offset. If uncommitted
// is true, the Reader will read uncommitted messages from the log. Otherwise,
// it will only return committed messages.
func (l *commitLog) NewReader(offset int64, uncommitted bool) (*Reader, error) {
	var (
		ctxReader contextReader
		err       error
	)
	if uncommitted {
		ctxReader, err = l.newReaderUncommitted(offset)
	} else {
		ctxReader, err = l.newReaderCommitted(offset)
	}
	return &Reader{
		ctxReader:   ctxReader,
		offset:      offset,
		log:         l,
		uncommitted: uncommitted,
	}, err
}

// ReadMessage reads a single message from the underlying CommitLog or blocks
// until one is available. It returns the SerializedMessage in addition to its
// offset, timestamp, and leader epoch. This may return uncommitted messages if
// the reader was created with the uncommitted flag set to true.
//
// ReadMessage should not be called concurrently, and the headersBuf slice
// should have a capacity of at least 28.
//
// TODO: Should this just return a MessageSet directly instead of a Message and
// the MessageSet header values?
func (r *Reader) ReadMessage(ctx context.Context, headersBuf []byte) (SerializedMessage, int64, int64, uint64, error) {
RETRY:
	msg, offset, timestamp, leaderEpoch, err := readMessage(ctx, r.ctxReader, headersBuf)
	if err != nil {
		if r.log.IsDeleted() {
			// The log was deleted while we were trying to read.
			return nil, 0, 0, 0, ErrCommitLogDeleted
		} else if r.log.IsClosed() {
			// The log was closed while we were trying to read.
			return nil, 0, 0, 0, ErrCommitLogClosed
		} else if pkgErrors.Cause(err) == ErrCommitLogReadonly && r.log.IsReadonly() {
			// The log was set to readonly while we were trying to read.
			return nil, 0, 0, 0, ErrCommitLogReadonly
		} else if pkgErrors.Cause(err) == ErrSegmentReplaced {
			// ErrSegmentReplaced indicates we attempted to read from a log
			// segment that was replaced due to compaction, so reinitialize the
			// contextReader and try again to read from the new segment.
			if r.uncommitted {
				r.ctxReader, err = r.log.newReaderUncommitted(r.offset)
			} else {
				r.ctxReader, err = r.log.newReaderCommitted(r.offset)
			}
			if err != nil {
				return nil, 0, 0, 0, pkgErrors.Wrap(err, "failed to reinitialize reader")
			}
			goto RETRY
		} else {
			return nil, 0, 0, 0, err
		}
	}
	r.offset = offset + 1
	return msg, offset, timestamp, leaderEpoch, err
}

type uncommittedReader struct {
	cl  *commitLog
	seg *segment
	mu  sync.Mutex
	pos int64
}

func (r *uncommittedReader) Read(ctx context.Context, p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		segments = r.cl.Segments()
		readSize int
		waiting  bool
	)

LOOP:
	for {
		readSize, err = r.seg.ReadAt(p[n:], r.pos)
		n += readSize
		r.pos += int64(readSize)
		if err != nil && err != io.EOF {
			break
		}
		if n == len(p) {
			break
		}
		if readSize != 0 && err == nil {
			waiting = false
			continue
		}

		// We hit the end of the segment.
		if err == io.EOF && !waiting {
			// Check if there are more segments.
			nextSeg := findSegmentByBaseOffset(segments, r.seg.BaseOffset+1)
			if nextSeg != nil {
				r.seg = nextSeg
				r.pos = 0
				continue
			}
			// Otherwise, wait for segment to be written to (or split).
			waiting = true
			if !r.waitForData(ctx, r.seg) {
				err = io.EOF
				break
			}
			// At this point, either the segment has more data or, if it was
			// full, a new segment was rolled. Try to read from the segment
			// again.
			continue
		}

		// We hit an EOF after waiting for data which means a new segment was
		// rolled, so move to the next segment.
		segments = r.cl.Segments()
		nextSeg := findSegmentByBaseOffset(segments, r.seg.BaseOffset+1)

		// If there are not enough segments to read, wait for new segment to be
		// appended or the context to be canceled.
		for nextSeg == nil {
			if !r.waitForData(ctx, r.seg) {
				err = io.EOF
				break LOOP
			}
			segments = r.cl.Segments()
			nextSeg = findSegmentByBaseOffset(segments, r.seg.BaseOffset+1)
		}
		r.seg = nextSeg
		r.pos = 0
		waiting = false
	}

	return n, err
}

func (r *uncommittedReader) waitForData(ctx context.Context, seg *segment) bool {
	wait := seg.WaitForData(r, r.pos)
	select {
	case <-r.cl.closed:
		seg.removeWaiter(r)
		return false
	case <-ctx.Done():
		seg.removeWaiter(r)
		return false
	case <-wait:
		return true
	}
}

// newReaderUncommitted returns a contextReader which reads data from the log
// starting at the given offset.
func (l *commitLog) newReaderUncommitted(offset int64) (contextReader, error) {
	seg, contains := findSegmentContains(l.Segments(), offset)
	if seg == nil {
		return nil, ErrSegmentNotFound
	}
	position := int64(0)
	if contains {
		e, err := seg.findEntry(offset)
		if err != nil {
			return nil, err
		}
		position = e.Position
	}
	return &uncommittedReader{
		cl:  l,
		seg: seg,
		pos: position,
	}, nil
}

type committedReader struct {
	cl    *commitLog
	seg   *segment
	hwSeg *segment
	mu    sync.Mutex
	pos   int64
	hwPos int64
	hw    int64
}

func (r *committedReader) Read(ctx context.Context, p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	segments := r.cl.Segments()

	// If seg is nil then the reader offset exceeded the HW, i.e. the log is
	// either empty or the offset overflows the HW. This means we need to wait
	// for data.
	if r.seg == nil {
		offset := r.hw + 1 // We want to read the next committed message.
		hw := r.cl.HighWatermark()
		for hw == r.hw {
			// The HW has not changed, so wait for it to update.
			err = r.waitForHW(ctx, hw)
			if err != nil {
				return
			}
			// Sync the HW.
			hw = r.cl.HighWatermark()
		}
		r.hw = hw
		segments = r.cl.Segments()
		hwIdx, hwPos, err := getHWPos(segments, r.hw)
		if err != nil {
			return 0, err
		}
		r.hwSeg = segments[hwIdx]
		r.hwPos = hwPos
		r.seg, _ = findSegment(segments, offset)
		if r.seg == nil {
			return 0, ErrSegmentNotFound
		}
		entry, err := r.seg.findEntry(offset)
		if err != nil {
			return 0, err
		}
		r.pos = entry.Position
	}

	return r.readLoop(ctx, p, segments)
}

func (r *committedReader) readLoop(
	ctx context.Context, p []byte, segments []*segment) (n int, err error) {

	var readSize int
LOOP:
	for {
		lim := int64(len(p[n:]))
		if r.seg == r.hwSeg {
			// If we're reading from the HW segment, read up to the HW pos.
			lim = min(lim, r.hwPos-r.pos)
		}
		readSize, err = r.seg.ReadAt(p[n:lim], r.pos)
		n += readSize
		r.pos += int64(readSize)
		if err != nil && err != io.EOF {
			break
		}
		if n == len(p) {
			break
		}
		if readSize != 0 && err == nil {
			continue
		}

		// We hit the end of the segment, so jump to the next one.
		if err == io.EOF {
			nextSeg := findSegmentByBaseOffset(segments, r.seg.BaseOffset+1)
			if nextSeg == nil {
				// QUESTION: Should this ever happen?
				err = errors.New("no segment to consume")
				break
			}
			r.seg = nextSeg
			r.pos = 0
			continue
		}

		// We hit the HW, so sync the latest.
		hw := r.cl.HighWatermark()
		for hw == r.hw {
			// The HW has not changed, so wait for it to update.
			err = r.waitForHW(ctx, hw)
			if err != nil {
				break LOOP
			}
			// Sync the HW.
			hw = r.cl.HighWatermark()
		}
		r.hw = hw
		segments = r.cl.Segments()
		hwIdx, hwPos, err := getHWPos(segments, r.hw)
		if err != nil {
			break
		}
		r.hwPos = hwPos
		r.hwSeg = segments[hwIdx]
	}

	return n, err
}

func (r *committedReader) waitForHW(ctx context.Context, hw int64) error {
	wait := r.cl.waitForHW(r, hw)
	select {
	case <-r.cl.closed:
		r.cl.removeHWWaiter(r)
		return io.EOF
	case <-ctx.Done():
		r.cl.removeHWWaiter(r)
		return io.EOF
	case readonly := <-wait:
		if readonly {
			return ErrCommitLogReadonly
		}
		return nil
	}
}

// newReaderCommitted returns a contextReader which reads only committed data
// from the log starting at the given offset.
func (l *commitLog) newReaderCommitted(offset int64) (contextReader, error) {
	var (
		hw       = l.HighWatermark()
		hwPos    = int64(-1)
		segments = l.Segments()
		hwSeg    *segment
	)

	// If offset exceeds HW, wait for the next message. This also covers the
	// case when the log is empty.
	if offset > hw || l.OldestOffset() == -1 {
		return &committedReader{
			cl:    l,
			seg:   nil,
			pos:   -1,
			hwSeg: hwSeg,
			hwPos: hwPos,
			hw:    hw,
		}, nil
	}

	if hw != -1 {
		hwIdx, hwPosition, err := getHWPos(segments, hw)
		if err != nil {
			return nil, err
		}
		hwPos = hwPosition
		hwSeg = segments[hwIdx]
	}

	position := int64(0)
	seg, contains := findSegmentContains(segments, offset)
	if contains {
		entry, err := seg.findEntry(offset)
		if err != nil {
			return nil, err
		}
		position = entry.Position
	}
	return &committedReader{
		cl:    l,
		seg:   seg,
		pos:   position,
		hwSeg: hwSeg,
		hwPos: hwPos,
		hw:    hw,
	}, nil
}

func getHWPos(segments []*segment, hw int64) (int, int64, error) {
	hwSeg, hwIdx := findSegment(segments, hw)
	if hwSeg == nil {
		return 0, 0, ErrSegmentNotFound
	}
	hwEntry, err := hwSeg.findEntry(hw)
	if err != nil {
		return 0, 0, err
	}
	return hwIdx, hwEntry.Position + int64(hwEntry.Size), nil
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

// ReverseReader reads messages in reverse order (newest to oldest) from a
// CommitLog. ReverseReaders should not be used concurrently.
type ReverseReader struct {
	log         *commitLog
	segments    []*segment
	segIdx      int // Current segment index (starts at last segment)
	scanner     *reverseSegmentScanner
	stopOffset  int64 // Stop reading at this offset (inclusive)
	uncommitted bool
}

// NewReverseReader creates a new ReverseReader starting at the given offset
// and reading backwards. If uncommitted is true, the Reader will read
// uncommitted messages from the log. Otherwise, it will only return committed
// messages (starting from HW).
func (l *commitLog) NewReverseReader(startOffset int64, uncommitted bool) (*ReverseReader, error) {
	segments := l.Segments()
	if len(segments) == 0 {
		return nil, ErrSegmentNotFound
	}

	var effectiveStart int64
	if uncommitted {
		effectiveStart = startOffset
	} else {
		// For committed reads, start from HW if startOffset exceeds it
		hw := l.HighWatermark()
		if hw == -1 {
			// Log is empty
			return nil, ErrSegmentNotFound
		}
		if startOffset > hw || startOffset == -1 {
			effectiveStart = hw
		} else {
			effectiveStart = startOffset
		}
	}

	// Find the segment containing the start offset
	seg, segIdx := findSegment(segments, effectiveStart)
	if seg == nil {
		return nil, ErrSegmentNotFound
	}

	return &ReverseReader{
		log:         l,
		segments:    segments,
		segIdx:      segIdx,
		scanner:     newReverseSegmentScanner(seg, effectiveStart),
		stopOffset:  -1, // Read all the way to the beginning by default
		uncommitted: uncommitted,
	}, nil
}

// NewReverseReaderFromEnd creates a new ReverseReader starting at the end of
// the log (either LEO for uncommitted or HW for committed).
func (l *commitLog) NewReverseReaderFromEnd(uncommitted bool) (*ReverseReader, error) {
	segments := l.Segments()
	if len(segments) == 0 {
		return nil, ErrSegmentNotFound
	}

	var startOffset int64
	if uncommitted {
		startOffset = l.NewestOffset()
	} else {
		startOffset = l.HighWatermark()
	}

	if startOffset == -1 {
		return nil, ErrSegmentNotFound
	}

	return l.NewReverseReader(startOffset, uncommitted)
}

// SetStopOffset sets the offset at which to stop reading (inclusive).
// Messages with offsets less than stopOffset will not be returned.
func (r *ReverseReader) SetStopOffset(offset int64) {
	r.stopOffset = offset
}

// ReadMessage reads the next message in reverse order (from newest to oldest).
// Returns io.EOF when there are no more messages or the stop offset is reached.
func (r *ReverseReader) ReadMessage(ctx context.Context, headersBuf []byte) (
	SerializedMessage, int64, int64, uint64, error) {

	for {
		select {
		case <-ctx.Done():
			return nil, 0, 0, 0, io.EOF
		default:
		}

		if r.log.IsDeleted() {
			return nil, 0, 0, 0, ErrCommitLogDeleted
		}
		if r.log.IsClosed() {
			return nil, 0, 0, 0, ErrCommitLogClosed
		}

		// Try to read from current segment
		msgSet, _, err := r.scanner.Scan()
		if err == io.EOF {
			// Move to previous segment
			if r.segIdx <= 0 {
				// No more segments
				return nil, 0, 0, 0, io.EOF
			}
			r.segIdx--
			r.scanner = newReverseSegmentScannerFromEnd(r.segments[r.segIdx])
			continue
		}
		if err != nil {
			return nil, 0, 0, 0, err
		}

		// Check stop offset
		offset := msgSet.Offset()
		if r.stopOffset >= 0 && offset < r.stopOffset {
			return nil, 0, 0, 0, io.EOF
		}

		// Extract message from message set
		msg := msgSet.Message()
		return msg, offset, msgSet.Timestamp(), msgSet.LeaderEpoch(), nil
	}
}
