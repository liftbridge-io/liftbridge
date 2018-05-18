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
			if !r.waitForData() {
				err = io.EOF
				break
			}
			continue
		}

		// If there are not enough segments to read, wait for new segment to be
		// appended or the context is canceled.
		segments = r.cl.Segments()
		for len(segments) <= r.idx+1 {
			if !r.waitForData() {
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

func (r *UncommittedReader) waitForData() bool {
	wait := make(chan struct{})
	r.cl.waitersMu.Lock()
	r.cl.dataWaiters[r] = wait
	r.cl.waitersMu.Unlock()
	select {
	case <-r.ctx.Done():
		r.cl.waitersMu.Lock()
		delete(r.cl.dataWaiters, r)
		r.cl.waitersMu.Unlock()
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
	cl         *CommitLog
	idx        int
	mu         sync.Mutex
	pos        int64
	ctx        context.Context
	hw         int64
	hwIdx      int
	hwPos      int64
	hwEndOfLog bool
}

func (r *CommittedReader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		segments = r.cl.Segments()
		segment  = segments[r.idx]
		readSize int
		hwIdx    int
		hwPos    int64
	)

LOOP:
	for {
		var checkHW bool

		// If the HW is in the current segment, read up to the HW pos.
		if r.hwIdx == r.idx {
			lim := min(r.hwPos, int64(len(p)))
			// We've read up to the HW, and the HW was at the end of the log.
			// Check to see if it's still at the end of the log.
			if lim == 0 && r.hwEndOfLog {
				segments = r.cl.Segments()
				idx, pos, e := getHWPos(segments, r.hw+1)
				if e != nil {
					// It's still at the end of the log, read til EOF.
					readSize, err = segment.ReadAt(p[n:], r.pos)
				} else {
					// It's no longer at the end of the log.
					r.hwIdx = idx
					r.hwPos = pos
					continue
				}
			} else {
				checkHW = true
				readSize, err = segment.ReadAt(p[n:lim], r.pos)
			}
		} else {
			readSize, err = segment.ReadAt(p[n:], r.pos)
		}
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

		// Check to see if we hit the end of the segment.
		if err == io.EOF {
			// If there are not enough segments to read, wait for new segment
			// to be appended or the context is canceled.
			for len(segments) <= r.idx+1 {
				if !r.waitForData() {
					err = io.EOF
					break LOOP
				}
				segments = r.cl.Segments()
			}
			r.idx++
			segment = segments[r.idx]
			r.pos = 0
		} else if checkHW {
			// We hit the HW, so sync the latest.
			segments = r.cl.Segments()
			hwIdx, hwPos, err = getHWPos(segments, r.cl.HighWatermark())
			if err != nil {
				return n, err
			}
			// The HW has not increased, so wait for it to update.
			for hwIdx == r.hwIdx && hwPos == r.hwPos {
				if !r.waitForHW() {
					err = io.EOF
					break LOOP
				}
				// Sync the HW.
				segments = r.cl.Segments()
				hwIdx, hwPos, err = getHWPos(segments, r.cl.HighWatermark())
				if err != nil {
					return n, err
				}
			}
			r.hwIdx = hwIdx
			r.hwPos = hwPos
		}
	}

	return
}

func (r *CommittedReader) waitForHW() bool {
	wait := make(chan struct{})
	r.cl.waitersMu.Lock()
	r.cl.hwWaiters[r] = wait
	r.cl.waitersMu.Unlock()
	select {
	case <-r.ctx.Done():
		r.cl.waitersMu.Lock()
		delete(r.cl.hwWaiters, r)
		r.cl.waitersMu.Unlock()
		return false
	case <-wait:
		return true
	}
}

func (r *CommittedReader) waitForData() bool {
	wait := make(chan struct{})
	r.cl.waitersMu.Lock()
	r.cl.dataWaiters[r] = wait
	r.cl.waitersMu.Unlock()
	select {
	case <-r.ctx.Done():
		r.cl.waitersMu.Lock()
		delete(r.cl.dataWaiters, r)
		r.cl.waitersMu.Unlock()
		return false
	case <-wait:
		return true
	}
}

func (l *CommitLog) NewReaderCommitted(ctx context.Context, offset int64) (io.Reader, error) {
	var (
		hw           = l.HighWatermark()
		segments     = l.Segments()
		newestOffset = l.NewestOffset()
		hwIdx        int
		hwPos        int64
		hwEndOfLog   bool
		err          error
	)
	if hw == -1 {
		// Log is empty.
		return &CommittedReader{
			cl:  l,
			ctx: ctx,
			hw:  hw,
		}, nil
	}
	if newestOffset == hw {
		// The HW is at the end of the log.
		hwIdx, hwPos, err = getHWPos(segments, hw)
		hwEndOfLog = true
	} else {
		// Add 1 because we want to read up to and including the HW.
		hwIdx, hwPos, err = getHWPos(segments, hw+1)
	}
	if err != nil {
		return nil, err
	}
	s, idx := findSegment(segments, offset)
	if s == nil {
		return nil, ErrSegmentNotFound
	}
	e, err := s.findEntry(offset)
	if err != nil {
		return nil, err
	}
	return &CommittedReader{
		cl:         l,
		idx:        idx,
		pos:        e.Position,
		ctx:        ctx,
		hw:         hw,
		hwIdx:      hwIdx,
		hwPos:      hwPos,
		hwEndOfLog: hwEndOfLog,
	}, nil
}

func getHWPos(segments []*Segment, hw int64) (int, int64, error) {
	s, idx := findSegment(segments, hw)
	if s == nil {
		return 0, 0, ErrSegmentNotFound
	}
	e, err := s.findEntry(hw)
	if err != nil {
		return 0, 0, err
	}
	return idx, e.Position, nil
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
