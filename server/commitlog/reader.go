package commitlog

import (
	"io"
	"sync"

	"golang.org/x/net/context"
)

type Reader struct {
	cl  *CommitLog
	idx int
	mu  sync.Mutex
	pos int64
	ctx context.Context
}

func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := r.cl.Segments()
	segment := segments[r.idx]

	var (
		readSize int
		waiting  bool
	)
LOOP:
	for {
		readSize, err = segment.ReadAt(p[n:], r.pos)
		n += readSize
		r.pos += int64(readSize)
		if readSize != 0 && err == nil {
			waiting = false
			continue
		}
		if n == len(p) || err != io.EOF {
			break
		}

		// Wait for segment to be written to (or split).
		if err == io.EOF && !waiting {
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

		r.idx++
		segment = segments[r.idx]
		r.pos = 0
	}

	return n, err
}

func (r *Reader) waitForData() bool {
	wait := make(chan struct{})
	r.cl.waitersMu.Lock()
	r.cl.waiters[r] = wait
	r.cl.waitersMu.Unlock()
	select {
	case <-r.ctx.Done():
		r.cl.waitersMu.Lock()
		delete(r.cl.waiters, r)
		r.cl.waitersMu.Unlock()
		return false
	case <-wait:
		return true
	}
}

func (l *CommitLog) NewReader(offset int64, maxBytes int32) (io.Reader, error) {
	s, idx := findSegment(l.Segments(), offset)
	if s == nil {
		return nil, ErrSegmentNotFound
	}
	e, err := s.findEntry(offset)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return &Reader{
		cl:  l,
		idx: idx,
		pos: e.Position,
		ctx: ctx,
	}, nil
}

func (l *CommitLog) NewReaderContext(ctx context.Context, offset int64) (io.Reader, error) {
	s, idx := findSegment(l.Segments(), offset)
	if s == nil {
		return nil, ErrSegmentNotFound
	}
	e, err := s.findEntry(offset)
	if err != nil {
		return nil, err
	}
	return &Reader{
		cl:  l,
		idx: idx,
		pos: e.Position,
		ctx: ctx,
	}, nil
}
