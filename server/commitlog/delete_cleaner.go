package commitlog

import (
	"time"

	"github.com/pkg/errors"

	"github.com/liftbridge-io/liftbridge/server/logger"
)

// computeTTL calculates the age cutoff for messages when there is an age
// retention policy. This function exists for mocking purposes.
var computeTTL = func(age time.Duration) int64 {
	return time.Now().Add(-age).UnixNano()
}

// deleteCleanerOptions contains configuration settings for the DeleteCleaner.
type deleteCleanerOptions struct {
	Retention struct {
		Bytes    int64
		Messages int64
		Age      time.Duration
	}
	Logger logger.Logger
	Name   string
}

// deleteCleaner implements the delete cleanup policy which deletes old log
// segments based on the retention policy.
type deleteCleaner struct {
	deleteCleanerOptions
}

// newDeleteCleaner returns a new cleaner which enforces log retention
// policies by deleting segments.
func newDeleteCleaner(opts deleteCleanerOptions) *deleteCleaner {
	return &deleteCleaner{opts}
}

// Clean will enforce the log retention policy by deleting old segments.
// Deletion only occurs at the segment granularity.
func (c *deleteCleaner) Clean(segments []*segment) ([]*segment, error) {
	var err error
	if len(segments) == 0 || c.noRetentionLimits() {
		return segments, nil
	}

	c.Logger.Debugf("Cleaning log %s based on retention policy %+v", c.Name, c.Retention)
	defer c.Logger.Debugf("Finished cleaning log %s", c.Name)

	// Limit by age first.
	if c.Retention.Age > 0 {
		segments, err = c.applyAgeLimit(segments)
		if err != nil {
			return nil, errors.Wrap(err, "failed to apply age retention limit")
		}
	}

	// Next limit by number of messages.
	if c.Retention.Messages > 0 {
		segments, err = c.applyMessagesLimit(segments)
		if err != nil {
			return nil, errors.Wrap(err, "failed to apply message retention limit")
		}
	}

	// Lastly limit by number of bytes.
	if c.Retention.Bytes > 0 {
		segments, err = c.applyBytesLimit(segments)
		if err != nil {
			return nil, errors.Wrap(err, "failed to apply bytes retention limit")
		}
	}

	return segments, nil
}

func (c *deleteCleaner) noRetentionLimits() bool {
	return c.Retention.Bytes == 0 && c.Retention.Messages == 0 && c.Retention.Age == 0
}

func (c *deleteCleaner) applyMessagesLimit(segments []*segment) ([]*segment, error) {
	// We must retain at least the active segment.
	if len(segments) <= 1 {
		return segments, nil
	}

	// We start at the most recent segment and work our way backwards until we
	// meet the retention size.
	var (
		lastSeg         = segments[len(segments)-1]
		cleanedSegments = []*segment{lastSeg}
		totalMessages   = lastSeg.MessageCount()
	)

	var i int
	for i = len(segments) - 2; i > -1; i-- {
		s := segments[i]
		totalMessages += s.MessageCount()
		if totalMessages > c.Retention.Messages {
			break
		}
		cleanedSegments = append([]*segment{s}, cleanedSegments...)
	}
	if i > -1 {
		for ; i > -1; i-- {
			// TODO: There is an edge case here where we fail partway through
			// deletion. We will delete some segments but return an error. This
			// should probably mark segments for deletion, remove them from the
			// read path, and then delete them asynchronously.
			if err := segments[i].Delete(); err != nil {
				return nil, err
			}
		}
	}

	return cleanedSegments, nil
}

func (c *deleteCleaner) applyBytesLimit(segments []*segment) ([]*segment, error) {
	// We must retain at least the active segment.
	if len(segments) <= 1 {
		return segments, nil
	}

	// We start at the most recent segment and work our way backwards until we
	// meet the retention size.
	var (
		lastSeg         = segments[len(segments)-1]
		cleanedSegments = []*segment{lastSeg}
		totalBytes      = lastSeg.Position()
	)

	var i int
	for i = len(segments) - 2; i > -1; i-- {
		s := segments[i]
		totalBytes += s.Position()
		if totalBytes > c.Retention.Bytes {
			break
		}
		cleanedSegments = append([]*segment{s}, cleanedSegments...)
	}
	if i > -1 {
		for ; i > -1; i-- {
			// TODO: There is an edge case here where we fail partway through
			// deletion. We will delete some segments but return an error. This
			// should probably mark segments for deletion, remove them from the
			// read path, and then delete them asynchronously.
			if err := segments[i].Delete(); err != nil {
				return nil, err
			}
		}
	}

	return cleanedSegments, nil
}

func (c *deleteCleaner) applyAgeLimit(segments []*segment) ([]*segment, error) {
	// We must retain at least the active segment.
	if len(segments) <= 1 {
		return segments, nil
	}

	var (
		ttl = computeTTL(c.Retention.Age)
		idx int
	)

	// Delete all segments whose last-written timestamp is less than the TTL
	// with the exception of the active (last) segment.
	for i, seg := range segments {
		if i != len(segments)-1 && seg.lastWriteTime < ttl {
			// TODO: There is an edge case here where we fail partway through
			// deletion. We will delete some segments but return an error. This
			// should probably mark segments for deletion, remove them from the
			// read path, and then delete them asynchronously.
			if err := seg.Delete(); err != nil {
				return nil, err
			}
		} else {
			idx = i
			break
		}
	}

	return segments[idx:], nil
}
