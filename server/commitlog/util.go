package commitlog

import "sort"

func findSegment(segments []*Segment, offset int64) (*Segment, int) {
	n := len(segments)
	idx := sort.Search(n, func(i int) bool {
		return segments[i].BaseOffset >= offset
	})
	if idx == n {
		return nil, idx
	}
	if segments[idx].BaseOffset == offset {
		return segments[idx], idx
	}
	idx--
	if idx < 0 {
		return nil, idx
	}
	return segments[idx], idx
}

func roundDown(total, factor int64) int64 {
	return factor * (total / factor)
}
