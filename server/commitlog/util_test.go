package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindSegment(t *testing.T) {
	segments := []*Segment{
		&Segment{BaseOffset: 0, lastOffset: int64Ptr(9)},
		&Segment{BaseOffset: 10, lastOffset: int64Ptr(19)},
		&Segment{BaseOffset: 20, lastOffset: int64Ptr(29)},
		&Segment{BaseOffset: 30, lastOffset: int64Ptr(39)},
		&Segment{BaseOffset: 40, lastOffset: int64Ptr(49)},
	}
	seg, idx := findSegment(segments, 0)
	require.Equal(t, 0, idx)
	require.Equal(t, segments[0], seg)

	seg, idx = findSegment(segments, 1)
	require.Equal(t, 0, idx)
	require.Equal(t, segments[0], seg)

	seg, idx = findSegment(segments, 15)
	require.Equal(t, 1, idx)
	require.Equal(t, segments[1], seg)

	seg, idx = findSegment(segments, 42)
	require.Equal(t, 4, idx)
	require.Equal(t, segments[4], seg)

	seg, idx = findSegment(segments, 60)
	require.Equal(t, 5, idx)
	require.Nil(t, seg)
}

func TestFindSegmentByBaseOffset(t *testing.T) {
	segments := []*Segment{
		&Segment{BaseOffset: 0},
		&Segment{BaseOffset: 10},
		&Segment{BaseOffset: 20},
		&Segment{BaseOffset: 30},
		&Segment{BaseOffset: 40},
	}
	require.Equal(t, segments[0], findSegmentByBaseOffset(segments, 0))
	require.Equal(t, segments[1], findSegmentByBaseOffset(segments, 1))
	require.Equal(t, segments[4], findSegmentByBaseOffset(segments, 39))
	require.Nil(t, findSegmentByBaseOffset(segments, 41))
}

func int64Ptr(i int64) *int64 {
	return &i
}
