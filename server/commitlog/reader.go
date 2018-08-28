package commitlog

import (
	"io"
	"sync"

	"github.com/liftbridge-io/liftbridge/server/proto"

	"golang.org/x/net/context"
)

// ReadMessage reads a single message from the given Reader or blocks until one
// is available. It returns the Message in addition to its offset and
// timestamp. The headersBuf slice should have a capacity of at least 20.
func ReadMessage(reader io.Reader, headersBuf []byte) (Message, int64, int64, error) {
	if _, err := reader.Read(headersBuf); err != nil {
		return nil, 0, 0, err
	}
	var (
		offset    = int64(proto.Encoding.Uint64(headersBuf[0:]))
		timestamp = int64(proto.Encoding.Uint64(headersBuf[8:]))
		size      = proto.Encoding.Uint32(headersBuf[16:])
		buf       = make([]byte, int(size))
	)
	if _, err := reader.Read(buf); err != nil {
		return nil, 0, 0, err
	}
	return Message(buf), offset, timestamp, nil
}

type UncommittedReader struct {
	cl  *CommitLog
	idx int
	mu  sync.Mutex
	pos int64
	ctx context.Context
}

func (r *UncommittedReader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		segments = r.cl.Segments()
		segment  = segments[r.idx]
		readSize int
		waiting  bool
	)

LOOP:
	for {
		readSize, err = segment.ReadAt(p[n:], r.pos)
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
			if len(segments) > r.idx+1 {
				goto NEXT_SEGMENT
			}
			// Otherwise, wait for segment to be written to (or split).
			waiting = true
			if !r.waitForData(segment) {
				err = io.EOF
				break
			}
			continue
		}

		// If there are not enough segments to read, wait for new segment to be
		// appended or the context to be canceled.
		segments = r.cl.Segments()
		for len(segments) <= r.idx+1 {
			if !r.waitForData(segment) {
				err = io.EOF
				break LOOP
			}
			segments = r.cl.Segments()
		}

	NEXT_SEGMENT:
		r.idx++
		segment = segments[r.idx]
		r.pos = 0
	}

	return n, err
}

func (r *UncommittedReader) waitForData(seg *Segment) bool {
	wait := seg.waitForData(r, r.pos)
	select {
	case <-r.cl.closed:
		seg.removeWaiter(r)
		return false
	case <-r.ctx.Done():
		seg.removeWaiter(r)
		return false
	case <-wait:
		return true
	}
}

// NewReaderUncommitted returns an io.Reader which reads data from the log
// starting at the given offset.
func (l *CommitLog) NewReaderUncommitted(ctx context.Context, offset int64) (io.Reader, error) {
	s, idx := findSegment(l.Segments(), offset)
	if s == nil {
		return nil, ErrSegmentNotFound
	}
	e, err := s.findEntry(offset)
	if err != nil {
		return nil, err
	}
	return &UncommittedReader{
		cl:  l,
		idx: idx,
		pos: e.Position,
		ctx: ctx,
	}, nil
}

type CommittedReader struct {
	cl    *CommitLog
	idx   int
	mu    sync.Mutex
	pos   int64
	ctx   context.Context
	hwIdx int
	hwPos int64
	hw    int64
}

func (r *CommittedReader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	segments := r.cl.Segments()

	// If idx is -1 then the reader offset exceeded the HW, i.e. the log is
	// either empty or the offset overflows the HW. This means we need to wait
	// for data.
	if r.idx == -1 {
		offset := r.hw + 1 // We want to read the next committed message.
		hw := r.cl.HighWatermark()
		for hw == r.hw {
			// The HW has not changed, so wait for it to update.
			if !r.waitForHW(hw) {
				err = io.EOF
				return
			}
			// Sync the HW.
			hw = r.cl.HighWatermark()
		}
		r.hw = hw
		segments = r.cl.Segments()
		r.hwIdx, r.hwPos, err = getHWPos(segments, r.hw)
		if err != nil {
			return
		}
		seg, idx := findSegment(segments, offset)
		if seg == nil {
			return 0, ErrSegmentNotFound
		}
		entry, err := seg.findEntry(offset)
		if err != nil {
			return 0, err
		}
		r.idx = idx
		r.pos = entry.Position
	}

	return r.readLoop(p, segments)
}

func (r *CommittedReader) readLoop(p []byte, segments []*Segment) (n int, err error) {
	var (
		readSize int
		segment  = segments[r.idx]
	)

LOOP:
	for {
		lim := int64(len(p))
		if r.idx == r.hwIdx {
			// If we're reading from the HW segment, read up to the HW pos.
			lim = min(lim, r.hwPos-r.pos)
		}
		readSize, err = segment.ReadAt(p[n:lim], r.pos)
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

		// We hit the end of the segment.
		if err == io.EOF {
			r.idx++
			segment = segments[r.idx]
			r.pos = 0
			continue
		}

		// We hit the HW, so sync the latest.
		hw := r.cl.HighWatermark()
		for hw == r.hw {
			// The HW has not changed, so wait for it to update.
			if !r.waitForHW(hw) {
				err = io.EOF
				break LOOP
			}
			// Sync the HW.
			hw = r.cl.HighWatermark()
		}
		r.hw = hw
		segments = r.cl.Segments()
		r.hwIdx, r.hwPos, err = getHWPos(segments, r.hw)
		if err != nil {
			break
		}
	}

	return n, err
}

func (r *CommittedReader) waitForHW(hw int64) bool {
	wait := r.cl.waitForHW(r, hw)
	select {
	case <-r.cl.closed:
		r.cl.removeHWWaiter(r)
		return false
	case <-r.ctx.Done():
		r.cl.removeHWWaiter(r)
		return false
	case <-wait:
		return true
	}
}

// NewReaderCommitted returns an io.Reader which reads only committed data from
// the log starting at the given offset.
func (l *CommitLog) NewReaderCommitted(ctx context.Context, offset int64) (io.Reader, error) {
	var (
		hw       = l.HighWatermark()
		hwIdx    = -1
		hwPos    = int64(-1)
		segments = l.Segments()
		err      error
	)
	if hw != -1 {
		hwIdx, hwPos, err = getHWPos(segments, hw)
		if err != nil {
			return nil, err
		}
	}

	// If offset exceeds HW, wait for the next message. This also covers the
	// case when the log is empty.
	if offset > hw {
		return &CommittedReader{
			cl:    l,
			idx:   -1,
			pos:   -1,
			hwIdx: hwIdx,
			hwPos: hwPos,
			ctx:   ctx,
			hw:    hw,
		}, nil
	}

	if oldest := l.OldestOffset(); offset < oldest {
		offset = oldest
	}
	seg, idx := findSegment(segments, offset)
	if seg == nil {
		return nil, ErrSegmentNotFound
	}
	entry, err := seg.findEntry(offset)
	if err != nil {
		return nil, err
	}
	return &CommittedReader{
		cl:    l,
		idx:   idx,
		pos:   entry.Position,
		hwIdx: hwIdx,
		hwPos: hwPos,
		ctx:   ctx,
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
