// Copyright 2017 Apcera Inc. All rights reserved.

package natslog

import (
	"bufio"
	"errors"
	"io"
	"runtime"
	"time"
)

const bufferSize = 4096

var ErrTimeout = errors.New("natslog: read timeout")

type timeoutReader struct {
	b         *bufio.Reader
	t         time.Time
	ch        <-chan error
	closeFunc func() error
}

func newTimeoutReader(r io.ReadCloser) *timeoutReader {
	return &timeoutReader{
		b:         bufio.NewReaderSize(r, bufferSize),
		closeFunc: func() error { return r.Close() },
	}
}

// SetDeadline sets the deadline for all future Read calls.
func (r *timeoutReader) SetDeadline(t time.Time) {
	r.t = t
}

func (r *timeoutReader) Read(b []byte) (n int, err error) {
	if r.ch == nil {
		if r.t.IsZero() || r.b.Buffered() > 0 {
			return r.b.Read(b)
		}
		ch := make(chan error, 1)
		r.ch = ch
		go func() {
			_, err := r.b.Peek(1)
			ch <- err
		}()
		runtime.Gosched()
	}
	if r.t.IsZero() {
		err = <-r.ch // Block
	} else {
		select {
		case err = <-r.ch: // Poll
		default:
			select {
			case err = <-r.ch: // Timeout
			case <-time.After(time.Until(r.t)):
				return 0, ErrTimeout
			}
		}
	}
	r.ch = nil
	if r.b.Buffered() > 0 {
		n, _ = r.b.Read(b)
	}
	return
}

func (r *timeoutReader) Close() error {
	return r.closeFunc()
}
