package commitlog

import (
	"github.com/cespare/xxhash"
)

type CompactCleaner struct {
	// map from key hash to offset
	m map[uint64]int64
}

func NewCompactCleaner() *CompactCleaner {
	return &CompactCleaner{
		m: make(map[uint64]int64),
	}
}

func (c *CompactCleaner) Clean(segments []*Segment) (cleaned []*Segment, err error) {
	if len(segments) == 0 {
		return segments, nil
	}

	var (
		ss *SegmentScanner
		ms MessageSet
	)

	// build the map of keys to their latest offsets
	for _, segment := range segments {
		ss = NewSegmentScanner(segment)

		for ms, err = ss.Scan(); err == nil; ms, err = ss.Scan() {
			c.m[Hash(ms.Message().Key())] = ms.Offset()
		}
	}

	// TODO: handle joining segments when they're smaller than max segment size
	for _, ds := range segments {
		ss = NewSegmentScanner(ds)

		cs, err := NewSegment(ds.path, ds.BaseOffset, ds.maxBytes, cleanedSuffix)
		if err != nil {
			return nil, err
		}

		for ms, err = ss.Scan(); err == nil; ms, err = ss.Scan() {
			var retain bool
			if c.m[Hash(ms.Message().Key())] <= ms.Offset() {
				retain = true
			}

			if retain {
				if _, err = cs.Write(ms, 1); err != nil {
					return nil, err
				}
			}
		}

		// TODO: need to update index?
		if err = cs.Replace(ds); err != nil {
			return nil, err
		}

		cleaned = append(cleaned, cs)
	}

	return cleaned, nil
}

func Hash(b []byte) uint64 {
	h := xxhash.New()
	if _, err := h.Write(b); err != nil {
		panic(err)
	}
	return h.Sum64()
}
