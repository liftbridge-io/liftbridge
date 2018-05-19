package commitlog

import (
	"io"
	"sync"

	"golang.org/x/net/context"
)

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
	case <-r.ctx.Done():
		seg.removeWaiter(r)
		return false
	case <-wait:
		return true
	}
}

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

	var (
		segments = r.cl.Segments()
		readSize int
		hw       int64
	)

	// If the log was empty, wait for data.
	if r.hw < 0 {
		hw = r.cl.HighWatermark()
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
	}

	segment := segments[r.idx]

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
		hw = r.cl.HighWatermark()
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
	case <-r.ctx.Done():
		r.cl.removeHWWaiter(r)
		return false
	case <-wait:
		return true
	}
}

func (l *CommitLog) NewReaderCommitted(ctx context.Context, offset int64) (io.Reader, error) {
	hw := l.HighWatermark()
	if hw == -1 {
		// The log is empty.
		return &CommittedReader{
			cl:    l,
			idx:   0,
			pos:   0,
			hwIdx: -1,
			hwPos: -1,
			ctx:   ctx,
			hw:    hw,
		}, nil
	}
	var (
		segments          = l.Segments()
		hwIdx, hwPos, err = getHWPos(segments, hw)
	)
	if err != nil {
		return nil, err
	}
	if offset > hw {
		offset = hw
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
