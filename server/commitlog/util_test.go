package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindSegment(t *testing.T) {
	segments := []*segment{
		{BaseOffset: 0, lastOffset: 9},
		{BaseOffset: 10, lastOffset: 19},
		{BaseOffset: 20, lastOffset: 29},
		{BaseOffset: 30, lastOffset: 39},
		{BaseOffset: 40, lastOffset: 49},
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
	segments := []*segment{
		{BaseOffset: 0},
		{BaseOffset: 10},
		{BaseOffset: 20},
		{BaseOffset: 30},
		{BaseOffset: 40},
	}
	require.Equal(t, segments[0], findSegmentByBaseOffset(segments, 0))
	require.Equal(t, segments[1], findSegmentByBaseOffset(segments, 1))
	require.Equal(t, segments[4], findSegmentByBaseOffset(segments, 39))
	require.Nil(t, findSegmentByBaseOffset(segments, 41))
}
