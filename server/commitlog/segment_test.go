package commitlog

import (
	"context"
	"os"
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

	_, err := s.write(make([]byte, 10), []*entry{{}})
	require.NoError(t, err)
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
	_, err := s.write(make([]byte, 5), []*entry{{}})
	require.NoError(t, err)
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
	_, err := s.write(make([]byte, 5), []*entry{{}})
	require.NoError(t, err)
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

// Ensure WaitForLEO returns a channel that is closed immediately when the
// expected LEO differs from the actual LEO. Otherwise returns a channel that
// is closed when the LEO changes or the segment is sealed.
func TestSegmentWaitForLEO(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	s := createSegment(t, dir, 0, 100)

	// Channel should be closed immediately if the expected LEO differs from
	// the actual.
	waiter := s.WaitForLEO(struct{}{}, 0, 1)
	select {
	case <-waiter:
	case <-time.After(time.Second):
		t.Fatal("Expected channel to be closed")
	}

	require.NoError(t, s.WriteMessageSet(make([]byte, 5), []*entry{{Offset: 0, Size: 5}}))
	require.NoError(t, s.WriteMessageSet(make([]byte, 5), []*entry{{Offset: 1, Size: 5}}))

	// Channel should be closed immediately if the expected LEO and actual last
	// known LEO are the same but the expected differs from the active
	// segment's last offset.
	waiter = s.WaitForLEO(struct{}{}, 0, 0)
	select {
	case <-waiter:
	case <-time.After(time.Second):
		t.Fatal("Expected channel to be closed")
	}

	// Channel should not be closed until segment is written to.
	waiter = s.WaitForLEO(struct{}{}, 1, 1)
	select {
	case <-waiter:
		t.Fatal("Channel was unexpectedly closed")
	default:
	}

	require.NoError(t, s.WriteMessageSet(make([]byte, 5), []*entry{{Offset: 2, Size: 5}}))

	// Channel should now be closed.
	select {
	case <-waiter:
	case <-time.After(time.Second):
		t.Fatal("Expected channel to be closed")
	}

	// Channel should not be closed until segment is sealed.
	waiter = s.WaitForLEO(struct{}{}, 2, 2)
	select {
	case <-waiter:
		t.Fatal("Channel was unexpectedly closed")
	default:
	}

	s.Seal()

	// Channel should now be closed.
	select {
	case <-waiter:
	case <-time.After(time.Second):
		t.Fatal("Expected channel to be closed")
	}

	require.NoError(t, s.WriteMessageSet(make([]byte, 100), []*entry{{Offset: 3, Size: 100}}))

	// Channel should be closed immediately because the segment is full.
	waiter = s.WaitForLEO(struct{}{}, 3, 3)
	select {
	case <-waiter:
	case <-time.After(time.Second):
		t.Fatal("Expected channel to be closed")
	}
}

// Ensure a corrupt index is detected and rebuilt from the log file.
func TestSegmentCorruptIndexRecovery(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	// Create a segment with some data
	s := createSegment(t, dir, 0, 1024)
	for i := 0; i < 5; i++ {
		writeToSegment(t, s, int64(i), []byte("test data"))
	}
	require.Equal(t, int64(5), s.MessageCount())
	require.Equal(t, int64(0), s.FirstOffset())
	require.Equal(t, int64(4), s.LastOffset())

	// Close the segment
	require.NoError(t, s.Close())

	// Corrupt the index by writing garbage to it
	indexPath := s.indexPath()
	require.NoError(t, corruptIndexFile(indexPath))

	// Reopen the segment - it should detect corruption and rebuild the index
	s2, err := newSegment(dir, 0, 1024, false, "")
	require.NoError(t, err)

	// Verify the index was rebuilt correctly
	require.Equal(t, int64(5), s2.MessageCount())
	require.Equal(t, int64(0), s2.FirstOffset())
	require.Equal(t, int64(4), s2.LastOffset())

	// Verify we can still read all messages
	scanner := newSegmentScanner(s2)
	for i := 0; i < 5; i++ {
		_, entry, err := scanner.Scan()
		require.NoError(t, err)
		require.Equal(t, int64(i), entry.Offset)
	}
}

// Ensure index rebuild works with an empty log file.
func TestSegmentCorruptIndexRecoveryEmptyLog(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	// Create an empty segment
	s := createSegment(t, dir, 0, 1024)
	require.NoError(t, s.Close())

	// Corrupt the index by writing garbage to it
	indexPath := s.indexPath()
	require.NoError(t, corruptIndexFile(indexPath))

	// Reopen the segment - it should detect corruption and rebuild the index
	s2, err := newSegment(dir, 0, 1024, false, "")
	require.NoError(t, err)

	// Verify the segment is still empty
	require.Equal(t, int64(0), s2.MessageCount())
	require.True(t, s2.IsEmpty())
}

// Ensure index rebuild handles truncated index (less entries than log has).
func TestSegmentCorruptIndexTruncated(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	// Create a segment with some data
	s := createSegment(t, dir, 0, 1024)
	for i := 0; i < 3; i++ {
		writeToSegment(t, s, int64(i), []byte("test data"))
	}
	require.NoError(t, s.Close())

	// Corrupt the index by truncating to just 1 entry (20 bytes)
	// The log has 3 messages but index will only have 1 entry
	indexPath := s.indexPath()
	require.NoError(t, truncateIndexFile(indexPath, 20))

	// Corrupt the single remaining entry to trigger rebuild
	require.NoError(t, corruptIndexFile(indexPath))

	// Reopen the segment - it should detect corruption and rebuild the index
	s2, err := newSegment(dir, 0, 1024, false, "")
	require.NoError(t, err)

	// Verify the index was rebuilt correctly from the log file
	require.Equal(t, int64(3), s2.MessageCount())
}

// corruptIndexFile writes garbage to an index file to corrupt it.
// The corruption makes the last entry have an offset < baseOffset.
func corruptIndexFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Read current file size to find the last entry
	info, err := f.Stat()
	if err != nil {
		return err
	}

	// Find the position of the last valid entry by scanning from the start
	// The index has 20-byte entries (entryWidth)
	entryWidth := int64(20)
	buf := make([]byte, entryWidth)
	var lastEntryPos int64 = -1

	for pos := int64(0); pos < info.Size(); pos += entryWidth {
		_, err := f.ReadAt(buf, pos)
		if err != nil {
			break
		}
		// Check if this entry is empty (all zeros for position, timestamp, size)
		// Entry format: offset(4) + timestamp(8) + position(4) + size(4)
		// Check if timestamp, position, and size are all zero
		isZero := true
		for i := 4; i < 20; i++ { // Skip offset, check rest
			if buf[i] != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			break
		}
		lastEntryPos = pos
	}

	if lastEntryPos < 0 {
		// No entries found, corrupt the first position
		lastEntryPos = 0
	}

	// Write garbage to make offset negative (< baseOffset of 0)
	// offset is stored as int32 at the start of each entry
	garbage := []byte{0xFF, 0xFF, 0xFF, 0xFF} // -1 as int32
	_, err = f.WriteAt(garbage, lastEntryPos)
	return err
}

// truncateIndexFile truncates an index file to the specified size.
func truncateIndexFile(path string, size int64) error {
	return os.Truncate(path, size)
}
