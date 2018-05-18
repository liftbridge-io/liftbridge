package server

import (
	"io"

	"golang.org/x/net/context"
)

type CommitLog interface {
	Delete() error
	NewReaderUncommitted(ctx context.Context, offset int64) (io.Reader, error)
	NewReaderCommitted(ctx context.Context, offset int64) (io.Reader, error)
	Truncate(int64) error
	NewestOffset() int64
	OldestOffset() int64
	SetHighWatermark(int64)
	HighWatermark() int64
	Append([]byte) (int64, error)
	Close() error
}
