package commitlog

import "github.com/liftbridge-io/liftbridge/server/logger"

type Cleaner interface {
	Clean([]*Segment) ([]*Segment, error)
}

// The delete cleaner implements the delete cleanup policy which
// deletes old log segments.

type DeleteCleaner struct {
	Retention struct {
		Bytes int64
	}
	log  logger.Logger
	name string
}

func NewDeleteCleaner(name string, bytes int64, logger logger.Logger) *DeleteCleaner {
	c := &DeleteCleaner{name: name, log: logger}
	c.Retention.Bytes = bytes
	return c
}

func (c *DeleteCleaner) Clean(segments []*Segment) ([]*Segment, error) {
	if len(segments) == 0 || c.Retention.Bytes == -1 {
		return segments, nil
	}

	c.log.Debugf("Cleaning log %s based on retention policy %+v", c.name, c.Retention)
	defer c.log.Debugf("Finished cleaning log %s", c.name)

	// we start at the most recent segment and work our way backwards until we meet the
	// retention size.
	cleanedSegments := []*Segment{segments[len(segments)-1]}
	totalBytes := cleanedSegments[0].Position()
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
