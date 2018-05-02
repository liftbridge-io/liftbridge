package server

import (
	"io"

	"golang.org/x/net/context"
)

type CommitLog interface {
	Delete() error
	NewReader(offset int64, maxBytes int32) (io.Reader, error)
	NewReaderContext(ctx context.Context, offset int64) (io.Reader, error)
	Truncate(int64) error
	NewestOffset() int64
	OldestOffset() int64
	Append([]byte) (int64, error)
	Close() error
}
