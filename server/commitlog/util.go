package commitlog

import "sort"

// findSegments returns the nearest segment whose base offset is greater than
// or equal to the given offset.
func findSegment(segments []*Segment, offset int64) (*Segment, int) {
	n := len(segments)
	idx := sort.Search(n, func(i int) bool {
		return segments[i].NextOffset() > offset
	})
	if idx == n {
		return nil, idx
	}
	return segments[idx], idx
}

// findSegmentIndexByTimestamp returns the index of the first segment whose
// base timestamp is greater than the given timestamp. Returns the index where
// the segment would be if there is no segment whose base timestamp is greater,
// i.e. the length of the slice.
func findSegmentIndexByTimestamp(segments []*Segment, timestamp int64) (int, error) {
	var (
		n   = len(segments)
		err error
	)
	idx := sort.Search(n, func(i int) bool {
		// Read the first entry in the segment to determine the base timestamp.
		var entry Entry
		if e := segments[i].Index.ReadEntryAtLogOffset(&entry, 0); e != nil {
			err = e
			return true
		}
		return entry.Timestamp > timestamp
	})
	return idx, err
}

func roundDown(total, factor int64) int64 {
	return factor * (total / factor)
}
