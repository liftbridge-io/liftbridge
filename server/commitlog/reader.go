package commitlog

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/liftbridge-io/liftbridge/server/proto"
	pkgErrors "github.com/pkg/errors"

	"golang.org/x/net/context"
)

type contextReader interface {
	Read(context.Context, []byte) (int, error)
}

// Reader reads messages atomically from a CommitLog. Readers should not be
// used concurrently.
type Reader struct {
	ctxReader   contextReader
	offset      int64
	log         *CommitLog
	uncommitted bool
}

// NewReader creates a new Reader starting at the given offset. If uncommitted
// is true, the Reader will read uncommitted messages from the log. Otherwise,
// it will only return committed messages.
func (l *CommitLog) NewReader(offset int64, uncommitted bool) (*Reader, error) {
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
// until one is available. It returns the Message in addition to its offset and
// timestamp. This may return uncommitted messages if the Reader was created
// with the uncommitted flag set to true.
//
// ReadMessage should not be called concurrently, and the headersBuf slice
// should have a capacity of at least 20.
func (r *Reader) ReadMessage(ctx context.Context, headersBuf []byte) (Message, int64, int64, error) {
RETRY:
	msg, offset, timestamp, err := r.readMessage(ctx, headersBuf)
	if err != nil {
		if pkgErrors.Cause(err) == ErrSegmentReplaced {
			// ErrSegmentReplaced indicates we attempted to read from a log segment
			// that was replaced due to compaction, so reinitialize the
			// contextReader and try again to read from the new segment.
			if r.uncommitted {
				r.ctxReader, err = r.log.newReaderUncommitted(r.offset)
			} else {
				r.ctxReader, err = r.log.newReaderCommitted(r.offset)
			}
			if err != nil {
				return nil, 0, 0, pkgErrors.Wrap(err, "failed to reinitialize reader")
			}
			goto RETRY
		} else {
			return nil, 0, 0, err
		}
	}
	r.offset = offset + 1
	return msg, offset, timestamp, err
}

func (r *Reader) readMessage(ctx context.Context, headersBuf []byte) (Message, int64, int64, error) {
	if _, err := r.ctxReader.Read(ctx, headersBuf); err != nil {
		return nil, 0, 0, pkgErrors.Wrap(err, "failed to read message headers")
	}
	var (
		offset    = int64(proto.Encoding.Uint64(headersBuf[0:]))
		timestamp = int64(proto.Encoding.Uint64(headersBuf[8:]))
		size      = proto.Encoding.Uint32(headersBuf[16:])
		buf       = make([]byte, int(size))
	)
	if _, err := r.ctxReader.Read(ctx, buf); err != nil {
		return nil, 0, 0, pkgErrors.Wrap(err, "failed to ready message payload")
	}
	m := Message(buf)
	// Check the CRC on the message.
	crc := m.Crc()
	if c := crc32.ChecksumIEEE(m[4:]); crc != c {
		// If the CRC doesn't match, data on disk is corrupted which means the
		// server is in an unrecoverable state.
		panic(fmt.Errorf("Read corrupted data, expected CRC: 0x%08x, got: 0x%08x", crc, c))
	}
	return m, offset, timestamp, nil
}

type uncommittedReader struct {
	cl  *CommitLog
	seg *Segment
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
			continue
		}

		// If there are not enough segments to read, wait for new segment to be
		// appended or the context to be canceled.
		segments = r.cl.Segments()
		nextSeg := findSegmentByBaseOffset(segments, r.seg.BaseOffset+1)
		for nextSeg == nil {
			if !r.waitForData(ctx, r.seg) {
				err = io.EOF
				break LOOP
			}
			segments = r.cl.Segments()
		}
		r.seg = nextSeg
	}

	return n, err
}

func (r *uncommittedReader) waitForData(ctx context.Context, seg *Segment) bool {
	wait := seg.waitForData(r, r.pos)
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
func (l *CommitLog) newReaderUncommitted(offset int64) (contextReader, error) {
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
	cl    *CommitLog
	seg   *Segment
	hwSeg *Segment
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
			if !r.waitForHW(ctx, hw) {
				err = io.EOF
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
	ctx context.Context, p []byte, segments []*Segment) (n int, err error) {

	var readSize int
LOOP:
	for {
		lim := int64(len(p))
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
			if !r.waitForHW(ctx, hw) {
				err = io.EOF
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

func (r *committedReader) waitForHW(ctx context.Context, hw int64) bool {
	wait := r.cl.waitForHW(r, hw)
	select {
	case <-r.cl.closed:
		r.cl.removeHWWaiter(r)
		return false
	case <-ctx.Done():
		r.cl.removeHWWaiter(r)
		return false
	case <-wait:
		return true
	}
}

// newReaderCommitted returns a contextReader which reads only committed data
// from the log starting at the given offset.
func (l *CommitLog) newReaderCommitted(offset int64) (contextReader, error) {
	var (
		hw       = l.HighWatermark()
		hwPos    = int64(-1)
		segments = l.Segments()
		hwSeg    *Segment
		err      error
	)

	// If offset exceeds HW, wait for the next message. This also covers the
	// case when the log is empty.
	if offset > hw {
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

	if oldest := l.OldestOffset(); offset < oldest {
		offset = oldest
	}
	seg, _ := findSegment(segments, offset)
	if seg == nil {
		return nil, ErrSegmentNotFound
	}
	entry, err := seg.findEntry(offset)
	if err != nil {
		return nil, err
	}
	return &committedReader{
		cl:    l,
		seg:   seg,
		pos:   entry.Position,
		hwSeg: hwSeg,
		hwPos: hwPos,
		hw:    hw,
	}, nil
}

func getHWPos(segments []*Segment, hw int64) (int, int64, error) {
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
