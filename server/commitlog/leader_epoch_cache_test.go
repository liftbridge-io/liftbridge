package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Ensure LeaderEpochCache correctly stores epoch offsets.
func TestLeaderEpochCache(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	l, err := newLeaderEpochCache("foo", dir, noopLogger())
	require.NoError(t, err)

	require.Equal(t, int64(-1), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, uint64(0), l.LastLeaderEpoch())

	require.NoError(t, l.Assign(1, 0))

	require.Equal(t, int64(-1), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(0), l.LastOffsetForLeaderEpoch(0))

	require.NoError(t, l.Assign(1, 10))

	require.Equal(t, int64(0), l.LastOffsetForLeaderEpoch(0))

	require.NoError(t, l.Assign(2, 10))
	require.NoError(t, l.Assign(3, 15))
	require.NoError(t, l.Assign(4, 30))
	require.NoError(t, l.Assign(5, 40))

	require.Equal(t, uint64(5), l.LastLeaderEpoch())

	require.NoError(t, l.ClearLatest(100))

	require.Equal(t, uint64(5), l.LastLeaderEpoch())

	require.NoError(t, l.ClearLatest(20))

	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(0), l.earliestOffset())
	require.Equal(t, int64(15), l.latestOffset())

	require.NoError(t, l.ClearEarliest(0))

	require.Equal(t, int64(0), l.earliestOffset())

	require.NoError(t, l.ClearEarliest(15))

	require.Equal(t, int64(15), l.earliestOffset())
	require.Equal(t, int64(15), l.latestOffset())

	require.NoError(t, l.ClearEarliest(16))

	require.Equal(t, int64(16), l.earliestOffset())
	require.Equal(t, int64(16), l.latestOffset())
}

// Ensure Rebase correctly applies the epoch offsets to the leaderEpochCache.
func TestLeaderEpochCacheRebase(t *testing.T) {
	dir1 := tempDir(t)
	defer remove(t, dir1)
	dir2 := tempDir(t)
	defer remove(t, dir2)

	l1, err := newLeaderEpochCache("foo", dir1, noopLogger())
	require.NoError(t, err)
	require.NoError(t, l1.Assign(3, 15))
	require.NoError(t, l1.Assign(4, 30))
	require.NoError(t, l1.Assign(5, 40))

	l2, err := newLeaderEpochCache("foo", dir2, noopLogger())
	require.NoError(t, err)
	require.NoError(t, l2.Assign(1, 0))
	require.NoError(t, l2.Assign(2, 10))

	require.NoError(t, l2.Rebase(l1, 3))

	require.Len(t, l2.epochOffsets, 5)
	require.Equal(t, int64(0), l2.earliestOffset())
	require.Equal(t, int64(40), l2.latestOffset())
	require.Equal(t, uint64(5), l2.LastLeaderEpoch())
}

// Ensure Replace correctly replaces the epoch offsets in the leaderEpochCache.
func TestLeaderEpochCacheReplace(t *testing.T) {
	dir1 := tempDir(t)
	defer remove(t, dir1)
	dir2 := tempDir(t)
	defer remove(t, dir2)

	l1, err := newLeaderEpochCache("foo", dir1, noopLogger())
	require.NoError(t, err)
	require.NoError(t, l1.Assign(3, 15))
	require.NoError(t, l1.Assign(4, 30))
	require.NoError(t, l1.Assign(5, 40))

	l2, err := newLeaderEpochCache("foo", dir2, noopLogger())
	require.NoError(t, err)
	require.NoError(t, l2.Assign(3, 17))
	require.NoError(t, l2.Assign(4, 33))

	require.NoError(t, l1.Replace(l2))

	require.Len(t, l1.epochOffsets, 2)
	require.Equal(t, int64(17), l1.earliestOffset())
	require.Equal(t, int64(33), l1.latestOffset())
	require.Equal(t, uint64(4), l1.LastLeaderEpoch())
}

// Ensure readLeaderEpochOffsets can correctly parse leader epoch checkpoint
// files.
func TestReadLeaderEpochOffsets(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	l, err := newLeaderEpochCache("foo", dir, noopLogger())
	require.NoError(t, err)

	require.NoError(t, l.Assign(1, 0))
	require.NoError(t, l.Assign(2, 10))
	require.NoError(t, l.Assign(3, 15))
	require.NoError(t, l.Assign(4, 30))
	require.NoError(t, l.Assign(5, 40))

	expected := []*epochOffset{
		{1, 0},
		{2, 10},
		{3, 15},
		{4, 30},
		{5, 40},
	}

	f, err := os.Open(filepath.Join(dir, leaderEpochFileName))
	require.NoError(t, err)

	offsets, err := readLeaderEpochOffsets(f)
	require.NoError(t, err)
	require.Equal(t, expected, offsets)
}
