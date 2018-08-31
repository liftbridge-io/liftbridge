package commitlog

import (
	"io"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/liftbridge-io/liftbridge/server/logger"
)

// computeTTL calculates the age cutoff for messages when there is an age
// retention policy. This function exists for mocking purposes.
var computeTTL = func(age time.Duration) int64 {
	return time.Now().Add(-age).UnixNano()
}

type Cleaner interface {
	Clean([]*Segment) ([]*Segment, error)
}

type DeleteCleanerOptions struct {
	Retention struct {
		Bytes    int64
		Messages int64
		Age      time.Duration
	}
	Logger logger.Logger
	Name   string
}

// DeleteCleaner implements the delete cleanup policy which deletes old log
// segments based on the retention policy.
type DeleteCleaner struct {
	DeleteCleanerOptions
}

func NewDeleteCleaner(opts DeleteCleanerOptions) *DeleteCleaner {
	return &DeleteCleaner{opts}
}

func (c *DeleteCleaner) Clean(segments []*Segment) ([]*Segment, error) {
	var err error
	if len(segments) == 0 || (c.Retention.Bytes == 0 && c.Retention.Messages == 0 && c.Retention.Age == 0) {
		return segments, nil
	}

	c.Logger.Debugf("Cleaning log %s based on retention policy %+v", c.Name, c.Retention)
	defer c.Logger.Debugf("Finished cleaning log %s", c.Name)

	// Limit by age first.
	if c.Retention.Age > 0 {
		segments, err = c.applyAgeLimit(segments)
		if err != nil {
			return nil, errors.Wrap(err, "failed to clean log")
		}
	}

	// Next limit by number of messages.
	if c.Retention.Messages > 0 {
		segments, err = c.applyMessagesLimit(segments)
		if err != nil {
			return nil, errors.Wrap(err, "failed to clean log")
		}
	}

	// Lastly limit by number of bytes.
	if c.Retention.Bytes > 0 {
		segments, err = c.applyBytesLimit(segments)
		if err != nil {
			return nil, errors.Wrap(err, "failed to clean log")
		}
	}

	return segments, nil
}

func (c DeleteCleaner) applyMessagesLimit(segments []*Segment) ([]*Segment, error) {
	// We start at the most recent segment and work our way backwards until we
	// meet the retention size.
	var (
		lastSeg         = segments[len(segments)-1]
		cleanedSegments = []*Segment{lastSeg}
		// NOTE: this won't work when compaction is enabled because there will be gaps.
		totalMessages = int64(lastSeg.NextOffset() - lastSeg.BaseOffset)
	)

	if len(segments) > 1 {
		var i int
		for i = len(segments) - 2; i > -1; i-- {
			s := segments[i]
			totalMessages += int64(s.NextOffset() - s.BaseOffset)
			if totalMessages > c.Retention.Messages {
				break
			}
			cleanedSegments = append([]*Segment{s}, cleanedSegments...)
		}
		if i > -1 {
			for ; i > -1; i-- {
				if err := segments[i].Delete(); err != nil {
					return nil, err
				}
			}
		}
	}

	return cleanedSegments, nil
}

func (c *DeleteCleaner) applyBytesLimit(segments []*Segment) ([]*Segment, error) {
	// We start at the most recent segment and work our way backwards until we
	// meet the retention size.
	var (
		lastSeg         = segments[len(segments)-1]
		cleanedSegments = []*Segment{lastSeg}
		totalBytes      = lastSeg.Position()
	)

	if len(segments) > 1 {
		var i int
		for i = len(segments) - 2; i > -1; i-- {
			s := segments[i]
			totalBytes += s.Position()
			if totalBytes > c.Retention.Bytes {
				break
			}
			cleanedSegments = append([]*Segment{s}, cleanedSegments...)
		}
		if i > -1 {
			for ; i > -1; i-- {
				if err := segments[i].Delete(); err != nil {
					return nil, err
				}
			}
		}
	}

	return cleanedSegments, nil
}

func (c *DeleteCleaner) applyAgeLimit(segments []*Segment) ([]*Segment, error) {
	// Do a binary search to find the first segment whose base timestamp is
	// greater than or equal to the TTL. Truncate all segments up to this point
	// since they are past the age limit.
	var (
		n   = len(segments)
		ttl = computeTTL(c.Retention.Age)
		err error
	)
	idx := sort.Search(n, func(i int) bool {
		// Read the first entry in the segment to determine the base timestamp.
		var entry Entry
		if e := segments[i].Index.ReadEntryAtLogOffset(&entry, 0); e != nil {
			// EOF means the segment is empty.
			if e != io.EOF {
				err = e
			}
			return true
		}
		return entry.Timestamp >= ttl
	})
	if err != nil {
		return nil, err
	}
	if idx == n {
		// All of the segments are expired. We must retain the last segment
		// still.
		idx = n - 1
	}

	// Truncate up to the index.
	for i := 0; i < idx; i++ {
		if err := segments[i].Delete(); err != nil {
			return nil, err
		}
	}

	return segments[idx:], nil
}
