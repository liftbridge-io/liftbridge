package commitlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Ensure CheckSplit returns false when the segment has not been written to and
// true when the log segment is full.
func TestSegmentCheckSplitFull(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	s := createSegment(t, dir, 0, 10)
	require.False(t, s.CheckSplit(1))

	s.write(make([]byte, 10), []*Entry{&Entry{}})
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
	s.write(make([]byte, 5), []*Entry{&Entry{}})
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
	s.write(make([]byte, 5), []*Entry{&Entry{}})
	s.firstWriteTime = 1
	require.True(t, s.CheckSplit(1))
}

type mockContextReader struct{}

func (m *mockContextReader) Read(ctx context.Context, buf []byte) (int, error) {
	return 0, nil
}

// Ensure Seal marks a Segment as sealed, notify waiters, and shrinks the
// index.
func TestSegmentSeal(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	s := createSegment(t, dir, 0, 10)
	// Ensure index file is pre-allocated to 10MB.
	stats, err := s.Index.file.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(10485760), stats.Size())

	// Add a waiter.
	ch := s.WaitForData(&mockContextReader{}, 0)

	s.Seal()

	require.True(t, s.sealed)
	// Ensure index was shrunk.
	stats, err = s.Index.file.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(0), stats.Size())

	// Ensure waiter is notified.
	select {
	case <-ch:
		return
	case <-time.After(time.Second):
		t.Fatal("Expected waiter to be notified")
	}
}

// Ensure calling Seal on a sealed Segment is a no-op.
func TestSegmentSealIdempotent(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	s := createSegment(t, dir, 0, 10)

	s.Seal()

	require.True(t, s.sealed)
	// Ensure index was shrunk.
	stats, err := s.Index.file.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(0), stats.Size())

	// Resize the index.
	require.NoError(t, s.Index.file.Truncate(256))

	s.Seal()

	// Size should be unchanged.
	stats, err = s.Index.file.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(256), stats.Size())
}
