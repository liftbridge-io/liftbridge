package commitlog

import (
	"context"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	headers = map[string][]byte{
		"foo": []byte("bar"),
	}
	msgs = []*Message{
		{Value: []byte("one"), Timestamp: 1, LeaderEpoch: 42, Headers: headers},
		{Value: []byte("two"), Timestamp: 2, LeaderEpoch: 42, Headers: headers},
		{Value: []byte("three"), Timestamp: 3, LeaderEpoch: 42, Headers: headers},
		{Value: []byte("four"), Timestamp: 4, LeaderEpoch: 42, Headers: headers},
		{Value: nil, Timestamp: 5, LeaderEpoch: 42, Headers: headers},
	}
)

func TestNewCommitLog(t *testing.T) {
	var err error
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	_, err = l.Append(msgs)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(0, true)
	require.NoError(t, err)

	headers := make([]byte, 28)
	for i, exp := range msgs {
		msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers, nil)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
		require.Equal(t, msgs[i].Timestamp, timestamp)
		require.Equal(t, msgs[i].LeaderEpoch, leaderEpoch)
		require.Equal(t, []byte("bar"), msg.Headers()["foo"])
		compareMessages(t, exp, msg)
	}
}

func TestNewCommitLogEmptyPath(t *testing.T) {
	_, err := New(Options{})
	require.Error(t, err)
}

func TestAppendMessageSet(t *testing.T) {
	var err error
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	set, _, err := newMessageSetFromProto(0, 0, msgs)
	require.NoError(t, err)

	offsets, err := l.AppendMessageSet(set)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4}, offsets)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(0, true)
	require.NoError(t, err)

	headers := make([]byte, 28)
	for i, exp := range msgs {
		msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers, nil)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
		require.Equal(t, msgs[i].Timestamp, timestamp)
		require.Equal(t, msgs[i].LeaderEpoch, leaderEpoch)
		compareMessages(t, exp, msg)
	}
}

func TestCommitLogRecover(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			opts := Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			}
			l, cleanup := setupWithOptions(t, opts)
			defer cleanup()

			// Append some messages.
			numMsgs := 10
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{Value: []byte(strconv.Itoa(i))}
			}
			for _, msg := range msgs {
				_, err := l.Append([]*Message{msg})
				require.NoError(t, err)
			}

			// Read them back as a sanity check.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := l.NewReader(0, true)
			require.NoError(t, err)

			headers := make([]byte, 28)
			for i, exp := range msgs {
				msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers, nil)
				require.NoError(t, err)
				compareMessages(t, exp, msg)
				require.Equal(t, int64(i), offset)
				require.Equal(t, msgs[i].Timestamp, timestamp)
				require.Equal(t, msgs[i].LeaderEpoch, leaderEpoch)
			}

			// Close the log and reopen, then ensure we read back the same
			// messages.
			require.NoError(t, l.Close())
			l, cleanup = setupWithOptions(t, opts)
			defer cleanup()
			defer l.Close()

			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			r, err = l.NewReader(0, true)
			require.NoError(t, err)
			for i, exp := range msgs {
				msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers, nil)
				require.NoError(t, err)
				compareMessages(t, exp, msg)
				require.Equal(t, int64(i), offset)
				require.Equal(t, msgs[i].Timestamp, timestamp)
				require.Equal(t, msgs[i].LeaderEpoch, leaderEpoch)
			}
		})
	}
}

func TestCommitLogRecoverHW(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
		MaxLogBytes:     100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()
	l.SetHighWatermark(100)
	require.Equal(t, int64(100), l.HighWatermark())
	require.NoError(t, l.Close())
	l, cleanup = setupWithOptions(t, opts)
	defer cleanup()
	defer l.Close()
	require.Equal(t, int64(100), l.HighWatermark())
}

func TestOverrideHighWatermark(t *testing.T) {
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	l.SetHighWatermark(100)
	require.Equal(t, int64(100), l.HighWatermark())
	l.OverrideHighWatermark(90)
	require.Equal(t, int64(90), l.HighWatermark())
}

func BenchmarkCommitLog(b *testing.B) {
	var err error
	l, cleanup := setup(b)
	defer l.Close()
	defer cleanup()

	for i := 0; i < b.N; i++ {
		_, err = l.Append(msgs)
		require.NoError(b, err)
	}
}

func TestOffsets(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 20,
	})
	defer l.Close()
	defer cleanup()
	require.Equal(t, int64(-1), l.OldestOffset())
	require.Equal(t, int64(-1), l.NewestOffset())

	numMsgs := 5
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{Value: []byte(strconv.Itoa(i))}
	}
	_, err := l.Append(msgs)
	require.NoError(t, err)

	require.Equal(t, int64(0), l.OldestOffset())
	require.Equal(t, int64(4), l.NewestOffset())
}

func TestDelete(t *testing.T) {
	l, cleanup := setup(t)
	defer cleanup()
	_, err := os.Stat(l.Path)
	require.False(t, os.IsNotExist(err))
	require.NoError(t, l.Delete())
	_, err = os.Stat(l.Path)
	require.True(t, os.IsNotExist(err))
}

func TestCleaner(t *testing.T) {
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	_, err := l.Append(msgs)
	require.NoError(t, err)
	segments := l.Segments()
	require.Equal(t, 1, len(l.Segments()))

	_, err = l.Append(msgs)
	require.NoError(t, err)

	require.NoError(t, l.Clean())

	require.Equal(t, 1, len(l.Segments()))
	for i, s := range l.Segments() {
		require.NotEqual(t, s, segments[i])
	}
}

// Ensure Clean deletes leader epoch offsets from the cache when segments are
// deleted but compaction is not run.
func TestCleanerDeleteLeaderEpochOffsets(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 6,
		MaxLogMessages:  5,
	})
	defer l.Close()
	defer cleanup()

	require.Equal(t, uint64(0), l.LastLeaderEpoch())
	require.Equal(t, int64(-1), l.LastOffsetForLeaderEpoch(0))

	// Add some messages.
	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i + 5)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 2,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i + 10)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 3,
		}})
		require.NoError(t, err)
	}

	require.Equal(t, 15, len(l.Segments()))

	require.Equal(t, 3, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(0), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(5), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(2))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(3))

	// Force a clean.
	require.NoError(t, l.Clean())

	require.Equal(t, 5, len(l.Segments()))
	require.Equal(t, int64(10), l.OldestOffset())
	require.Equal(t, int64(14), l.NewestOffset())
	require.Equal(t, 1, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(2))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(3))
}

// Ensure Clean replaces leader epoch offsets in the cache when segments are
// compacted.
func TestCleanerReplaceLeaderEpochOffsets(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 6,
		Compact:         true,
	})
	defer l.Close()
	defer cleanup()

	require.Equal(t, uint64(0), l.LastLeaderEpoch())
	require.Equal(t, int64(-1), l.LastOffsetForLeaderEpoch(0))

	// Add some messages.
	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Key:         []byte("foo"),
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Key:         []byte("bar"),
			Value:       []byte(strconv.Itoa(i + 5)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 2,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Key:         []byte("baz"),
			Value:       []byte(strconv.Itoa(i + 10)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 3,
		}})
		require.NoError(t, err)
	}

	// Set the HW so compaction will run.
	l.SetHighWatermark(l.NewestOffset())

	require.Equal(t, 15, len(l.Segments()))

	require.Equal(t, 3, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(0), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(5), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(2))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(3))

	// Force a clean.
	require.NoError(t, l.Clean())

	require.Equal(t, 3, len(l.Segments()))
	require.Equal(t, int64(4), l.OldestOffset())
	require.Equal(t, int64(14), l.NewestOffset())
	require.Equal(t, 3, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(4), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(9), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(2))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(3))
}

// Ensures OffsetForTimestamp returns the earliest offset whose timestamp is
// greater than or equal to the given timestamp.
func TestOffsetForTimestamp(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	// Append some messages.
	numMsgs := 10
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{Value: []byte(strconv.Itoa(i)), Timestamp: int64(i * 10)}
	}
	for _, msg := range msgs {
		_, err := l.Append([]*Message{msg})
		require.NoError(t, err)
	}

	// Underflowed timestamp should return the first offset.
	offset, err := l.OffsetForTimestamp(-1)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	// Find offset in the first segment.
	offset, err = l.OffsetForTimestamp(20)
	require.NoError(t, err)
	require.Equal(t, int64(2), offset)

	// Find offset in an inner segment.
	offset, err = l.OffsetForTimestamp(30)
	require.NoError(t, err)
	require.Equal(t, int64(3), offset)

	// Find offset in the last segment.
	offset, err = l.OffsetForTimestamp(90)
	require.NoError(t, err)
	require.Equal(t, int64(9), offset)

	// Overflowed timestamp should return the next offset.
	offset, err = l.OffsetForTimestamp(500)
	require.NoError(t, err)
	require.Equal(t, int64(10), offset)

	// Find offset for timestamp not present in log. Should return offset with
	// next highest timestamp.
	offset, err = l.OffsetForTimestamp(25)
	require.NoError(t, err)
	require.Equal(t, int64(3), offset)
}

// Ensure Truncate removes log entries up to the given offset and that the
// leader epoch cache is also truncated.
func TestTruncate(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 6,
	})
	defer l.Close()
	defer cleanup()

	// Add some messages.
	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i + 5)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 2,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i + 10)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 3,
		}})
		require.NoError(t, err)
	}

	require.Equal(t, int64(14), l.NewestOffset())
	require.Equal(t, 3, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(0), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(5), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(2))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(3))

	require.NoError(t, l.Truncate(7))

	require.Equal(t, int64(6), l.NewestOffset())
	require.Equal(t, 2, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(2), l.LastLeaderEpoch())
	require.Equal(t, int64(0), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(5), l.LastOffsetForLeaderEpoch(1))
}

// Ensure NotifyLEO returns a closed channel when the given offset is not the
// current log end offset.
func TestNotifyLEOMismatch(t *testing.T) {
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	// Add some messages.
	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}

	// Get current log end offset and then add another message.
	leo := l.NewestOffset()
	_, err := l.Append([]*Message{{
		Value:       []byte(strconv.Itoa(5)),
		Timestamp:   time.Now().UnixNano(),
		LeaderEpoch: 1,
	}})
	require.NoError(t, err)

	// Notify LEO should return a closed channel because the LEO is different
	// than the expected LEO.
	waiter := struct{}{}
	ch := l.NotifyLEO(waiter, leo)
	select {
	case <-ch:
	default:
		t.Fatalf("Expected closed channel")
	}
}

// Ensure NotifyLEO returns a channel that is closed once more data is written
// to the log past the log end offset.
func TestNotifyLEONewData(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256,
	})
	defer l.Close()
	defer cleanup()

	// Add some messages.
	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}

	// Get current log end offset.
	leo := l.NewestOffset()

	// Register a waiter.
	waiter := struct{}{}
	ch := l.NotifyLEO(waiter, leo)

	select {
	case <-ch:
		t.Fatalf("Unexpected channel close")
	default:
	}

	// Add another message.
	_, err := l.Append([]*Message{{
		Value:       []byte(strconv.Itoa(5)),
		Timestamp:   time.Now().UnixNano(),
		LeaderEpoch: 1,
	}})
	require.NoError(t, err)

	select {
	case <-ch:
	default:
		t.Fatalf("Expected channel to close")
	}
}

// Ensure NotifyLEO returns the same channel if the waiter is already
// registered in the log.
func TestNotifyLEOIdempotent(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256,
	})
	defer l.Close()
	defer cleanup()

	// Get current log end offset.
	leo := l.NewestOffset()

	// Register a waiter.
	waiter := struct{}{}
	ch1 := l.NotifyLEO(waiter, leo)

	// Register the same waiter again. This should return the same channel.
	ch2 := l.NotifyLEO(waiter, leo)

	require.Equal(t, ch1, ch2)
}

func setup(t require.TestingT) (*commitLog, func()) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 6,
		MaxLogBytes:     30,
	}
	return setupWithOptions(t, opts)
}

func setupWithOptions(t require.TestingT, opts Options) (*commitLog, func()) {
	l, err := New(opts)
	require.NoError(t, err)
	return l.(*commitLog), func() {
		remove(t, opts.Path)
	}
}

func tempDir(t require.TestingT) string {
	p, err := ioutil.TempDir("", "lift_")
	require.NoError(t, err)
	return p
}

func remove(t require.TestingT, path string) {
	require.NoError(t, os.RemoveAll(path))
}
