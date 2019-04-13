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
// up to but excluding the active (last) segment or the provided HW, whichever
// comes first.
func (c *CompactCleaner) Clean(hw int64, segments []*Segment) ([]*Segment, error) {
	if len(segments) <= 1 {
		return segments, nil
	}

	c.Logger.Debugf("Compacting log %s", c.Name)
	compacted, removed, err := c.compact(hw, segments)
	if err == nil {
		c.Logger.Debugf("Finished compacting log %s, removed %d messages", c.Name, removed)
	}

	return compacted, errors.Wrap(err, "failed to compact log")

}

func (c *CompactCleaner) compact(hw int64, segments []*Segment) ([]*Segment, int, error) {
	// Compact messages up to the last segment or HW, whichever is first, by
	// scanning keys and retaining only the latest.
	// TODO: Implement option for configuring minimum compaction lag.
	var (
		compacted     = make([]*Segment, 0, len(segments))
		removed       = 0
		keysToOffsets = make(map[string]int64)
	)
SCAN:
	for _, segment := range segments {
		ss := NewSegmentScanner(segment)
		for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
			offset := ms.Offset()
			if offset > hw {
				break SCAN
			}
			keysToOffsets[string(ms.Message().Key())] = offset
		}
	}

	// Write new segments. Skip the last segment since we will not compact it.
	// TODO: Join segments that are below the bytes limit.
	for _, seg := range segments[:len(segments)-1] {
		cs, err := seg.Cleaned()
		if err != nil {
			return nil, 0, err
		}
		ss := NewSegmentScanner(seg)
		for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
			var (
				offset       = ms.Offset()
				key          = ms.Message().Key()
				latestOffset = keysToOffsets[string(key)]
			)
			// Retain all messages with no keys and last message for each key.
			// Also retain all messages after the HW.
			if key == nil || offset == latestOffset || offset >= hw {
				entries := EntriesForMessageSet(cs.Position(), ms)
				if err := cs.WriteMessageSet(ms, entries); err != nil {
					return nil, 0, err
				}
			} else {
				removed++
			}
		}

		if cs.IsEmpty() {
			// If the new segment is empty, remove it along with the old one.
			if err := cleanupEmptySegment(cs, seg); err != nil {
				return nil, 0, err
			}
		} else {
			// Otherwise replace the old segment with the compacted one.
			if err = cs.Replace(seg); err != nil {
				return nil, 0, err
			}
			compacted = append(compacted, cs)
		}
	}
	// Add the last segment back in to the compacted list.
	compacted = append(compacted, segments[len(segments)-1])
	return compacted, removed, nil
}

func cleanupEmptySegment(new, old *Segment) error {
	// Delete the new segment if it's empty.
	if err := new.Delete(); err != nil {
		return err
	}
	// Also delete the old segment since it's been compacted. Set the replaced
	// flag since this is in the read path.
	old.Lock()
	old.replaced = true
	old.Unlock()
	return old.Delete()
}
