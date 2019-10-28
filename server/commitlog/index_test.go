package commitlog

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexInitialSize(t *testing.T) {
	dir := tempDir(t)
	defer os.RemoveAll(dir)

	// Create a new index.
	idx, err := newIndex(options{path: dir + "test.idx"})
	require.NoError(t, err)
	entry, err := idx.InitializePosition()
	require.NoError(t, err)
	require.Nil(t, entry)

	// Verify the index is preallocated and the position is 0.
	require.Equal(t, int64(10*1024*1024), idx.size)
	require.Equal(t, int64(0), idx.position)

	// Verify the recorded size matches the file size.
	finfo, err := idx.file.Stat()
	require.NoError(t, err)
	require.Equal(t, idx.size, finfo.Size())
}

func TestIndexExistingSize(t *testing.T) {
	dir := tempDir(t)
	defer os.RemoveAll(dir)

	// Create a new index.
	idx, err := newIndex(options{path: dir + "test.idx"})
	require.NoError(t, err)
	e, err := idx.InitializePosition()
	require.NoError(t, err)
	require.Nil(t, e)

	// Write some entries.
	err = idx.writeEntries([]*entry{
		{}, {}, {}, {},
	})
	require.NoError(t, err)

	// Close the index so that it will be truncated.
	err = idx.Close()
	require.NoError(t, err)

	// Reopen the index and verify the size and position are correct.
	idx, err = newIndex(options{path: dir + "test.idx"})
	require.NoError(t, err)
	require.Equal(t, idx.size, int64(entryWidth*4))
	require.Equal(t, idx.size, idx.position)
}

func TestIndexExpansion(t *testing.T) {
	dir := tempDir(t)
	defer os.RemoveAll(dir)

	// Create an index with enough room for a single entry.
	idx, err := newIndex(options{path: dir + "test.idx", bytes: entryWidth})
	require.NoError(t, err)
	e, err := idx.InitializePosition()
	require.NoError(t, err)
	require.Nil(t, e)

	// Write two entries.
	writeEntry := entry{Offset: 123, Timestamp: 456, Position: 789, Size: 987}
	err = idx.writeEntries([]*entry{{}, &writeEntry})
	require.NoError(t, err)

	// Check the second entry can be retrieved.
	var readEntry entry
	err = idx.ReadEntryAtLogOffset(&readEntry, 1)
	require.NoError(t, err)
	require.Equal(t, writeEntry, readEntry)
}
