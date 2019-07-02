// Copyright 2017 Apcera Inc. All rights reserved.

package natslog

import (
	"io"
	"testing"
	"time"
)

func TestTimeoutReader(t *testing.T) {
	reader, writer := io.Pipe()
	r := newTimeoutReader(reader)

	r.SetDeadline(time.Now().Add(time.Millisecond))
	n, err := r.Read(make([]byte, 10))
	if err != ErrTimeout {
		t.Fatal("expected ErrTimeout")
	}
	if n != 0 {
		t.Fatalf("expected: 0\ngot: %d", n)
	}

	writer.Write([]byte("hello"))
	r.SetDeadline(time.Now().Add(time.Millisecond))
	buf := make([]byte, 5)
	n, err = r.Read(buf)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected: 5\ngot: %d", n)
	}
	if string(buf) != "hello" {
		t.Fatalf("expected: hello\ngot: %s", buf)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("error: %v", err)
	}

	n, err = r.Read(make([]byte, 5))
	if err != io.ErrClosedPipe {
		t.Fatalf("expected: ErrClosedPipe\ngot: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected: 0\ngot: %d", n)
	}
}
