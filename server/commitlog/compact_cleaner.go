package commitlog

import (
	"github.com/pkg/errors"

	"github.com/liftbridge-io/liftbridge/server/logger"
)

// CompactCleanerOptions contains configuration settings for the
// CompactCleaner.
type CompactCleanerOptions struct {
	Logger logger.Logger
	Name   string
}

// CompactCleaner implements the compaction policy which replaces segments with
// compacted ones, i.e. retaining only the last message for a given key.
type CompactCleaner struct {
	CompactCleanerOptions
}

// NewCompactCleaner returns a new Cleaner which performs log compaction by
// rewriting segments such that they contain only the last message for a given
// key.
func NewCompactCleaner(opts CompactCleanerOptions) *CompactCleaner {
	return &CompactCleaner{opts}
}

// Clean performs log compaction by rewriting segments such that they contain
// only the last message for a given key. Compaction is applied to all segments
// up to but excluding the active (last) segment.
func (c *CompactCleaner) Clean(segments []*Segment) ([]*Segment, error) {
	if len(segments) <= 1 {
		return segments, nil
	}

	c.Logger.Debugf("Compacting log %s", c.Name)
	defer c.Logger.Debugf("Finished compacting log %s", c.Name)

	var err error
	segments, err = c.compact(segments)
	return segments, errors.Wrap(err, "failed to compact log")

}

func (c *CompactCleaner) compact(segments []*Segment) (compacted []*Segment, err error) {
	// Compact messages up to the last segment by scanning keys and retaining
	// only the latest.
	// TODO: Implement option for configuring minimum compaction lag.
	keysToOffsets := make(map[string]int64)
	for _, segment := range segments[:len(segments)-1] {
		ss := NewSegmentScanner(segment)
		for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
			keysToOffsets[string(ms.Message().Key())] = ms.Offset()
		}
	}

	// Write new segments.
	// TODO: Join segments that are below the bytes limit.
	for _, seg := range segments[:len(segments)-1] {
		cs, err := seg.Cleaner()
		if err != nil {
			return nil, err
		}
		ss := NewSegmentScanner(seg)
		for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
			latestOffset := keysToOffsets[string(ms.Message().Key())]
			// Retain all messages with no keys and last message for each key.
			if ms.Message().Key() == nil || latestOffset == ms.Offset() {
				entries := EntriesForMessageSet(cs.NextOffset(), cs.Position(), ms)
				if err := cs.WriteMessageSet(ms, entries); err != nil {
					return nil, err
				}
			}
		}

		if err = cs.Replace(seg); err != nil {
			return nil, err
		}
		compacted = append(compacted, cs)
	}
	compacted = append(compacted, segments[len(segments)-1])
	return compacted, nil
}
