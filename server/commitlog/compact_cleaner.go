package commitlog

import (
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/liftbridge-io/liftbridge/server/logger"
)

const defaultCompactMaxGoroutines = 10

// CompactCleanerOptions contains configuration settings for the
// CompactCleaner.
type CompactCleanerOptions struct {
	Logger        logger.Logger
	Name          string
	MaxGoroutines int
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
	if opts.MaxGoroutines == 0 {
		opts.MaxGoroutines = defaultCompactMaxGoroutines
	}
	return &CompactCleaner{opts}
}

// Compact performs log compaction by rewriting segments such that they contain
// only the last message for a given key. Compaction is applied to all segments
// up to but excluding the active (last) segment or the provided HW, whichever
// comes first.
func (c *CompactCleaner) Compact(hw int64, segments []*Segment) ([]*Segment, error) {
	if len(segments) <= 1 {
		return segments, nil
	}

	c.Logger.Debugf("Compacting log %s", c.Name)
	before := time.Now()
	compacted, removed, err := c.compact(hw, segments)
	if err == nil {
		c.Logger.Debugf("Finished compacting log %s\n"+
			"\tMessages Removed: %d\n"+
			"\tSegments: %d -> %d\n"+
			"\tDuration: %s",
			c.Name, removed, len(segments), len(compacted), time.Since(before))
	}

	return compacted, errors.Wrap(err, "failed to compact log")

}

type keyOffset struct {
	sync.RWMutex
	offset int64
}

func (k *keyOffset) set(offset int64) {
	k.Lock()
	if offset > k.offset {
		k.offset = offset
	}
	k.Unlock()
}

func (k *keyOffset) get() int64 {
	k.RLock()
	defer k.RUnlock()
	return k.offset
}

func (c *CompactCleaner) compact(hw int64, segments []*Segment) ([]*Segment, int, error) {
	// Compact messages up to the last segment or HW, whichever is first, by
	// scanning keys and retaining only the latest.
	// TODO: Implement option for configuring minimum compaction lag.
	var (
		compacted  = make([]*Segment, 0, len(segments))
		removed    = 0
		keyOffsets = c.scanKeys(hw, segments)
	)

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
				latest, ok   = keyOffsets.Load(string(key))
				latestOffset int64
			)
			if ok {
				latestOffset = latest.(*keyOffset).get()
			}

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

func (c *CompactCleaner) scanKeys(hw int64, segments []*Segment) *sync.Map {
	var (
		wg            sync.WaitGroup
		keyOffsets    = new(sync.Map)
		numGoroutines = c.MaxGoroutines
		segmentC      = make(chan *Segment, len(segments))
	)
	if len(segments) < numGoroutines {
		numGoroutines = len(segments)
	}

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go c.scanSegments(hw, segmentC, &wg, keyOffsets)
	}

	for _, seg := range segments {
		segmentC <- seg
	}
	close(segmentC)

	wg.Wait()
	return keyOffsets
}

func (c *CompactCleaner) scanSegments(hw int64, ch <-chan *Segment, wg *sync.WaitGroup, keyOffsets *sync.Map) {
LOOP:
	for seg := range ch {
		ss := NewSegmentScanner(seg)
		for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
			offset := ms.Offset()
			if offset > hw {
				break LOOP
			}
			curr, loaded := keyOffsets.LoadOrStore(
				string(ms.Message().Key()), &keyOffset{offset: offset})
			if loaded {
				curr.(*keyOffset).set(offset)
			}
		}
	}
	wg.Done()
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
