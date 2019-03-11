package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/liftbridge-io/liftbridge/server/proto"
)

type keyValue struct {
	key   []byte
	value []byte
}

type expectedMsg struct {
	Offset int64
	Msg    *proto.Message
}

// Ensure Clean is a no-op when there are no segments.
func TestCompactCleanerNoSegments(t *testing.T) {
	opts := CompactCleanerOptions{Name: "foo", Logger: noopLogger()}
	cleaner := NewCompactCleaner(opts)
	segments, err := cleaner.Clean(nil)
	require.NoError(t, err)
	require.Nil(t, segments)
}

// Ensure Clean is a no-op when there is one segment.
func TestCompactCleanerOneSegment(t *testing.T) {
	opts := CompactCleanerOptions{Name: "foo", Logger: noopLogger()}
	cleaner := NewCompactCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	expected := []*Segment{createSegment(t, dir, 0, 100)}
	actual, err := cleaner.Clean(expected)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

// Ensure Clean retains only the latest message for each key up to the last
// segment.
func TestCompactCleaner(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
		Compact:         true,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	// Append some messages.
	entries := []keyValue{
		keyValue{[]byte("foo"), []byte("first")},
		keyValue{[]byte("bar"), []byte("first")},
		keyValue{[]byte("foo"), []byte("second")},
		keyValue{[]byte("foo"), []byte("third")},
		keyValue{[]byte("bar"), []byte("second")},
		keyValue{[]byte("baz"), []byte("first")},
		keyValue{[]byte("baz"), []byte("second")},
		keyValue{[]byte("qux"), []byte("first")},
		keyValue{[]byte("foo"), []byte("fourth")},
		keyValue{[]byte("baz"), []byte("third")},
	}
	appendToLog(t, l, entries)

	// Force a compaction.
	require.NoError(t, l.clean())

	expected := []*expectedMsg{
		&expectedMsg{Offset: 4, Msg: &proto.Message{Key: []byte("bar"), Value: []byte("second")}},
		&expectedMsg{Offset: 6, Msg: &proto.Message{Key: []byte("baz"), Value: []byte("second")}},
		&expectedMsg{Offset: 7, Msg: &proto.Message{Key: []byte("qux"), Value: []byte("first")}},
		&expectedMsg{Offset: 8, Msg: &proto.Message{Key: []byte("foo"), Value: []byte("fourth")}},
		// This one is present because it's in the active segment.
		&expectedMsg{Offset: 9, Msg: &proto.Message{Key: []byte("baz"), Value: []byte("third")}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(0, true)
	require.NoError(t, err)
	headers := make([]byte, 20)
	for _, exp := range expected {
		msg, offset, _, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err)
		require.Equal(t, exp.Offset, offset)
		compareMessages(t, exp.Msg, msg)
	}
}

// Ensure Clean retains all messages that do not have keys.
func TestCompactCleanerNoKeys(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
		Compact:         true,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	// Append some messages.
	entries := []keyValue{
		keyValue{nil, []byte("first")},
		keyValue{nil, []byte("second")},
		keyValue{nil, []byte("third")},
		keyValue{nil, []byte("fourth")},
	}
	appendToLog(t, l, entries)

	// Force a compaction.
	require.NoError(t, l.clean())

	expected := []*expectedMsg{
		&expectedMsg{Offset: 0, Msg: &proto.Message{Value: []byte("first")}},
		&expectedMsg{Offset: 1, Msg: &proto.Message{Value: []byte("second")}},
		&expectedMsg{Offset: 2, Msg: &proto.Message{Value: []byte("third")}},
		&expectedMsg{Offset: 3, Msg: &proto.Message{Value: []byte("fourth")}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(0, true)
	require.NoError(t, err)
	headers := make([]byte, 20)
	for _, exp := range expected {
		msg, offset, _, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err)
		require.Equal(t, exp.Offset, offset)
		compareMessages(t, exp.Msg, msg)
	}
}

func appendToLog(t *testing.T, l *CommitLog, entries []keyValue) {
	for _, entry := range entries {
		msg := &proto.Message{
			Key:   entry.key,
			Value: entry.value,
		}
		_, err := l.Append([]*proto.Message{msg})
		require.NoError(t, err)
	}
}
