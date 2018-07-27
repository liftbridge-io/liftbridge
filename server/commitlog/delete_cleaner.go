package commitlog

import "github.com/liftbridge-io/liftbridge/server/logger"

type Cleaner interface {
	Clean([]*Segment) ([]*Segment, error)
}

type DeleteCleanerOptions struct {
	Retention struct {
		Bytes    int64
		Messages int64
	}
	Logger logger.Logger
	Name   string
}

// DeleteCleaner implements the delete cleanup policy which deletes old log
// segments.
type DeleteCleaner struct {
	DeleteCleanerOptions
}

func NewDeleteCleaner(opts DeleteCleanerOptions) *DeleteCleaner {
	return &DeleteCleaner{opts}
}

func (c *DeleteCleaner) Clean(segments []*Segment) ([]*Segment, error) {
	var err error
	if len(segments) == 0 || (c.Retention.Bytes == 0 && c.Retention.Messages == 0) {
		return segments, nil
	}

	c.Logger.Debugf("Cleaning log %s based on retention policy %+v", c.Name, c.Retention)
	defer c.Logger.Debugf("Finished cleaning log %s", c.Name)

	// Limit by number of messages first.
	if c.Retention.Messages > 0 {
		segments, err = c.applyMessagesLimit(segments)
		if err != nil {
			return nil, err
		}
	}

	// Limit by number of bytes.
	if c.Retention.Bytes > 0 {
		segments, err = c.applyBytesLimit(segments)
		if err != nil {
			return nil, err
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
				s := segments[i]
				if err := s.Delete(); err != nil {
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
				s := segments[i]
				if err := s.Delete(); err != nil {
					return nil, err
				}
			}
		}
	}

	return cleanedSegments, nil
}
