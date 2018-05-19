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
	cl  *CommitLog
	idx int
	mu  sync.Mutex
	pos int64
	ctx context.Context
}

func (r *CommittedReader) Read(p []byte) (n int, err error) {
	// TODO
	return 0, nil
}

func (l *CommitLog) NewReaderCommitted(ctx context.Context, offset int64) (io.Reader, error) {
	// TODO
	return nil, nil
}
