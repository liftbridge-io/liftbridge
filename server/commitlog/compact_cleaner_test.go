package commitlog

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

type keyValue struct {
	key   []byte
	value []byte
}

type expectedMsg struct {
	Offset int64
	Msg    *Message
}

// Ensure Compact is a no-op when there are no segments.
func TestCompactCleanerNoSegments(t *testing.T) {
	opts := compactCleanerOptions{Name: "foo", Logger: noopLogger()}
	cleaner := newCompactCleaner(opts)
	segments, epochCache, err := cleaner.Compact(0, nil)
	require.NoError(t, err)
	require.Nil(t, segments)
	require.Nil(t, epochCache)
}

// Ensure Compact is a no-op when there is one segment.
func TestCompactCleanerOneSegment(t *testing.T) {
	opts := compactCleanerOptions{Name: "foo", Logger: noopLogger()}
	cleaner := newCompactCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	expected := []*segment{createSegment(t, dir, 0, 100)}
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
		{[]byte("foo"), []byte("first")},
		{[]byte("bar"), []byte("first")},
		{[]byte("foo"), []byte("second")},
		{[]byte("foo"), []byte("third")},
		{[]byte("bar"), []byte("second")},
		{[]byte("baz"), []byte("first")},
		{[]byte("baz"), []byte("second")},
		{[]byte("qux"), []byte("first")},
		{[]byte("foo"), []byte("fourth")},
		{[]byte("baz"), []byte("third")},
	}
	appendToLog(t, l, entries, true)

	// Force a compaction.
	require.NoError(t, l.Clean())

	expected := []*expectedMsg{
		{Offset: 4, Msg: &Message{Key: []byte("bar"), Value: []byte("second")}},
		{Offset: 7, Msg: &Message{Key: []byte("qux"), Value: []byte("first")}},
		{Offset: 8, Msg: &Message{Key: []byte("foo"), Value: []byte("fourth")}},
		// This one is present because it's in the active segment.
		{Offset: 9, Msg: &Message{Key: []byte("baz"), Value: []byte("third")}},
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
		{[]byte("foo"), []byte("first")},
		{[]byte("bar"), []byte("first")},
		{[]byte("foo"), []byte("second")},
		{[]byte("foo"), []byte("third")},
		{[]byte("bar"), []byte("second")},
		{[]byte("baz"), []byte("first")},
		{[]byte("baz"), []byte("second")},
		{[]byte("qux"), []byte("first")},
		{[]byte("foo"), []byte("fourth")},
		{[]byte("baz"), []byte("third")},
	}
	appendToLog(t, l, entries, false)
	l.SetHighWatermark(5)

	// Force a compaction.
	require.NoError(t, l.Clean())

	expected := []*expectedMsg{
		{Offset: 3, Msg: &Message{Key: []byte("foo"), Value: []byte("third")}},
		{Offset: 4, Msg: &Message{Key: []byte("bar"), Value: []byte("second")}},
		{Offset: 5, Msg: &Message{Key: []byte("baz"), Value: []byte("first")}},
		// These are retained because they are after the HW.
		{Offset: 6, Msg: &Message{Key: []byte("baz"), Value: []byte("second")}},
		{Offset: 7, Msg: &Message{Key: []byte("qux"), Value: []byte("first")}},
		{Offset: 8, Msg: &Message{Key: []byte("foo"), Value: []byte("fourth")}},
		{Offset: 9, Msg: &Message{Key: []byte("baz"), Value: []byte("third")}},
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
		{nil, []byte("first")},
		{nil, []byte("second")},
		{nil, []byte("third")},
		{nil, []byte("fourth")},
	}
	appendToLog(t, l, entries, true)

	// Force a compaction.
	require.NoError(t, l.Clean())

	expected := []*expectedMsg{
		{Offset: 0, Msg: &Message{Value: []byte("first")}},
		{Offset: 1, Msg: &Message{Value: []byte("second")}},
		{Offset: 2, Msg: &Message{Value: []byte("third")}},
		{Offset: 3, Msg: &Message{Value: []byte("fourth")}},
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
		{[]byte("foo"), []byte("first")},
		{[]byte("bar"), []byte("first")},
		{[]byte("foo"), []byte("second")},
		{[]byte("foo"), []byte("third")},
		{[]byte("bar"), []byte("second")},
		{[]byte("baz"), []byte("first")},
		{[]byte("baz"), []byte("second")},
		{[]byte("qux"), []byte("first")},
		{[]byte("foo"), []byte("fourth")},
		{[]byte("baz"), []byte("third")},
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
					msg := &Message{
						Key:   []byte(keys[rand.Intn(len(keys))]),
						Value: buf,
					}
					offsets, _, err := l.Append([]*Message{msg})
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

func appendToLog(t *testing.T, l *commitLog, entries []keyValue, commit bool) {
	for _, entry := range entries {
		msg := &Message{
			Key:   entry.key,
			Value: entry.value,
		}
		offsets, _, err := l.Append([]*Message{msg})
		require.NoError(t, err)
		if commit {
			l.SetHighWatermark(offsets[len(offsets)-1])
		}
	}
}
