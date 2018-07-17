package server

import (
	"io"

	"golang.org/x/net/context"

	"github.com/liftbridge-io/liftbridge/server/proto"
)

// CommitLog is the durable write-ahead log interface used to back each stream.
type CommitLog interface {
	// Delete closes the log and removes all data associated with it from the
	// filesystem.
	Delete() error

	// NewReaderUncommitted returns an io.Reader which reads data from the log
	// starting at the given offset.
	NewReaderUncommitted(ctx context.Context, offset int64) (io.Reader, error)

	// NewReaderCommitted returns an io.Reader which reads only committed data
	// from the log starting at the given offset.
	NewReaderCommitted(ctx context.Context, offset int64) (io.Reader, error)

	// Truncate removes all messages from the log starting at the given offset.
	Truncate(offset int64) error

	// NewestOffset returns the offset of the last message in the log.
	NewestOffset() int64

	// OldestOffset returns the offset of the first message in the log.
	OldestOffset() int64

	// SetHighWatermark sets the high watermark on the log. All messages up to
	// and including the high watermark are considered committed.
	SetHighWatermark(hw int64)

	// HighWatermark returns the high watermark for the log.
	HighWatermark() int64

	// Append writes the given batch of messages to the log and returns their
	// corresponding offsets in the log.
	Append(msg []*proto.Message) ([]int64, error)

	// AppendMessageSet writes the given message set data to the log and
	// returns the corresponding offsets in the log.
	AppendMessageSet(ms []byte) ([]int64, error)

	// Close closes each log segment file and stops the background goroutine
	// checkpointing the high watermark to disk.
	Close() error
}
