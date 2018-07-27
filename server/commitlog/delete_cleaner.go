package commitlog

import "github.com/liftbridge-io/liftbridge/server/logger"

type Cleaner interface {
	Clean([]*Segment) ([]*Segment, error)
}

type DeleteCleanerOptions struct {
	Retention struct {
		Bytes int64
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
	if len(segments) == 0 || c.Retention.Bytes == 0 {
		return segments, nil
	}

	c.Logger.Debugf("Cleaning log %s based on retention policy %+v", c.Name, c.Retention)
	defer c.Logger.Debugf("Finished cleaning log %s", c.Name)

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
			if c.Retention.Bytes > 0 && totalBytes > c.Retention.Bytes {
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
