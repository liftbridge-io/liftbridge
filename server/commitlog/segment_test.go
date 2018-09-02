package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Ensure CheckSplit returns false when the segment has not been written to and
// true when the log segment is full.
func TestSegmentCheckSplitFull(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	s := createSegment(t, dir, 0, 10)
	require.False(t, s.CheckSplit(1))

	s.Write(make([]byte, 10), []*Entry{&Entry{}})
	require.True(t, s.CheckSplit(1))
}

// Ensure CheckSplit returns false when LogRollTime is 0 and the segment is not
// full.
func TestSegmentCheckSplitLogRollTimeZero(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	s := createSegment(t, dir, 0, 10)
	require.False(t, s.CheckSplit(0))
}

// Ensure CheckSplit returns false when the segment is not full and LogRollTime
// has not been exceeded.
func TestSegmentCheckSplitNotFull(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)
	timestampBefore := timestamp
	timestamp = func() int64 {
		return 2
	}
	defer func() {
		timestamp = timestampBefore
	}()

	s := createSegment(t, dir, 0, 10)
	s.Write(make([]byte, 5), []*Entry{&Entry{}})
	s.firstWriteTime = 1
	require.False(t, s.CheckSplit(5))
}

// Ensure CheckSplit returns true when the segment is not full but LogRollTime
// has been exceeded.
func TestSegmentCheckSplitLogRollTimeExceeded(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)
	timestampBefore := timestamp
	timestamp = func() int64 {
		return 2
	}
	defer func() {
		timestamp = timestampBefore
	}()

	s := createSegment(t, dir, 0, 10)
	s.Write(make([]byte, 5), []*Entry{&Entry{}})
	s.firstWriteTime = 1
	require.True(t, s.CheckSplit(1))
}
