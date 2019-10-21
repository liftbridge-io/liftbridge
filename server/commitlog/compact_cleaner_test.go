package commitlog

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

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

// Ensure Compact is a no-op when there are no segments.
func TestCompactCleanerNoSegments(t *testing.T) {
	opts := CompactCleanerOptions{Name: "foo", Logger: noopLogger()}
	cleaner := NewCompactCleaner(opts)
	segments, epochCache, err := cleaner.Compact(0, nil)
	require.NoError(t, err)
	require.Nil(t, segments)
	require.Nil(t, epochCache)
}

// Ensure Compact is a no-op when there is one segment.
func TestCompactCleanerOneSegment(t *testing.T) {
	opts := CompactCleanerOptions{Name: "foo", Logger: noopLogger()}
	cleaner := NewCompactCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	expected := []*Segment{createSegment(t, dir, 0, 100)}
	actual, epochCache, err := cleaner.Compact(0, expected)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.Nil(t, epochCache)
}

// Ensure Compact retains only the latest message for each key up to the last
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
	appendToLog(t, l, entries, true)

	// Force a compaction.
	require.NoError(t, l.Clean())

	expected := []*expectedMsg{
		&expectedMsg{Offset: 4, Msg: &proto.Message{Key: []byte("bar"), Value: []byte("second")}},
		&expectedMsg{Offset: 7, Msg: &proto.Message{Key: []byte("qux"), Value: []byte("first")}},
		&expectedMsg{Offset: 8, Msg: &proto.Message{Key: []byte("foo"), Value: []byte("fourth")}},
		// This one is present because it's in the active segment.
		&expectedMsg{Offset: 9, Msg: &proto.Message{Key: []byte("baz"), Value: []byte("third")}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(0, true)
	require.NoError(t, err)
	headers := make([]byte, 28)
	for _, exp := range expected {
		msg, offset, _, _, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err)
		require.Equal(t, exp.Offset, offset)
		compareMessages(t, exp.Msg, msg)
	}
}

// Ensure Compact retains only the latest message for each key up to the HW.
func TestCompactCleanerHW(t *testing.T) {
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
	appendToLog(t, l, entries, false)
	l.SetHighWatermark(5)

	// Force a compaction.
	require.NoError(t, l.Clean())

	expected := []*expectedMsg{
		&expectedMsg{Offset: 3, Msg: &proto.Message{Key: []byte("foo"), Value: []byte("third")}},
		&expectedMsg{Offset: 4, Msg: &proto.Message{Key: []byte("bar"), Value: []byte("second")}},
		&expectedMsg{Offset: 5, Msg: &proto.Message{Key: []byte("baz"), Value: []byte("first")}},
		// These are retained because they are after the HW.
		&expectedMsg{Offset: 6, Msg: &proto.Message{Key: []byte("baz"), Value: []byte("second")}},
		&expectedMsg{Offset: 7, Msg: &proto.Message{Key: []byte("qux"), Value: []byte("first")}},
		&expectedMsg{Offset: 8, Msg: &proto.Message{Key: []byte("foo"), Value: []byte("fourth")}},
		&expectedMsg{Offset: 9, Msg: &proto.Message{Key: []byte("baz"), Value: []byte("third")}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(0, true)
	require.NoError(t, err)
	headers := make([]byte, 28)
	for _, exp := range expected {
		msg, offset, _, _, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err)
		require.Equal(t, exp.Offset, offset)
		compareMessages(t, exp.Msg, msg)
	}
}

// Ensure Compact retains all messages that do not have keys.
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
	appendToLog(t, l, entries, true)

	// Force a compaction.
	require.NoError(t, l.Clean())

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
	headers := make([]byte, 28)
	for _, exp := range expected {
		msg, offset, _, _, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err)
		require.Equal(t, exp.Offset, offset)
		compareMessages(t, exp.Msg, msg)
	}
}

// Ensure neither log truncation nor compaction fail when run concurrently.
func TestCompactCleanerTruncateConcurrent(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
		Compact:         true,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	stop := make(chan bool)
	wait := make(chan bool)
	go func() {
		for {
			select {
			case <-stop:
				wait <- true
				return
			default:
				require.NoError(t, l.Truncate(0))
			}
		}
	}()

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
	appendToLog(t, l, entries, true)

	stop <- true
	<-wait

	// Force a compaction.
	require.NoError(t, l.Clean())
	require.NoError(t, l.Truncate(0))

	require.Equal(t, int64(-1), l.OldestOffset())
}

func BenchmarkClean1GBSegments(b *testing.B) {
	benchmarkClean(b, 1024*1024*1024)
}

func BenchmarkClean512MBSegments(b *testing.B) {
	benchmarkClean(b, 1024*1024*512)
}

func BenchmarkClean256MBSegments(b *testing.B) {
	benchmarkClean(b, 1024*1024*256)
}

func BenchmarkClean128MBSegments(b *testing.B) {
	benchmarkClean(b, 1024*1024*128)
}

func BenchmarkClean64MBSegments(b *testing.B) {
	benchmarkClean(b, 1024*1024*64)
}

var msgSizes = []int{
	128,
	1024,
	1024 * 64,
}

func benchmarkClean(b *testing.B, segmentSize int64) {
	for _, msgSize := range msgSizes {
		b.Run(fmt.Sprintf("%d", msgSize), func(subB *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				opts := Options{
					Path:            tempDir(b),
					MaxSegmentBytes: segmentSize,
					Compact:         true,
				}
				l, cleanup := setupWithOptions(b, opts)
				defer cleanup()

				keys := []string{"foobar", "foo", "bar", "baz", "qux", "quux", "quuz"}

				buf := make([]byte, msgSize)
				for i := 0; i < 200000; i++ {
					msg := &proto.Message{
						Key:   []byte(keys[rand.Intn(len(keys))]),
						Value: buf,
					}
					offsets, err := l.Append([]*proto.Message{msg})
					require.NoError(b, err)
					l.SetHighWatermark(offsets[len(offsets)-1])
				}

				b.StartTimer()
				require.NoError(b, l.Clean())
			}
			b.StopTimer()
		})
	}
}

func appendToLog(t *testing.T, l *CommitLog, entries []keyValue, commit bool) {
	for _, entry := range entries {
		msg := &proto.Message{
			Key:   entry.key,
			Value: entry.value,
		}
		offsets, err := l.Append([]*proto.Message{msg})
		require.NoError(t, err)
		if commit {
			l.SetHighWatermark(offsets[len(offsets)-1])
		}
	}
}
