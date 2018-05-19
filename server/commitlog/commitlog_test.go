package commitlog_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/tylertreat/liftbridge/server/commitlog"
)

var (
	msgs = []commitlog.Message{
		commitlog.NewMessage([]byte("one")),
		commitlog.NewMessage([]byte("two")),
		commitlog.NewMessage([]byte("three")),
		commitlog.NewMessage([]byte("four")),
	}
	msgSets = []commitlog.MessageSet{
		commitlog.NewMessageSet(0, msgs...),
		commitlog.NewMessageSet(1, msgs...),
	}
)

func TestNewCommitLog(t *testing.T) {
	var err error
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	for _, exp := range msgSets {
		_, err = l.Append(exp)
		require.NoError(t, err)
	}
	maxBytes := msgSets[0].Size()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReaderUncommitted(ctx, 0)
	require.NoError(t, err)

	for i, exp := range msgSets {
		p := make([]byte, maxBytes)
		_, err = r.Read(p)
		require.NoError(t, err)

		act := commitlog.MessageSet(p)
		require.Equal(t, exp, act)
		require.Equal(t, int64(i), act.Offset())

		payload := act.Payload()
		var offset int
		for _, msg := range msgs {
			require.Equal(t, []byte(msg), payload[offset:offset+len(msg)])
			offset += len(msg)
		}
	}
}

func TestCommitLogRecover(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			opts := commitlog.Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
				MaxLogBytes:     -1,
			}
			l, cleanup := setupWithOptions(t, opts)
			defer cleanup()

			// Append some messages.
			numMsgs := 10
			msgs := make([]commitlog.MessageSet, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = commitlog.NewMessageSet(
					uint64(i), commitlog.NewMessage([]byte(strconv.Itoa(i))),
				)
			}
			for _, ms := range msgs {
				_, err := l.Append(ms)
				require.NoError(t, err)
			}

			// Read them back as a sanity check.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := l.NewReaderUncommitted(ctx, 0)
			require.NoError(t, err)
			for _, ms := range msgs {
				buf := make([]byte, ms.Size())
				_, err := r.Read(buf)
				require.NoError(t, err)
				require.Equal(t, ms, commitlog.MessageSet(buf))
			}

			// Close the log and reopen, then ensure we read back the same
			// messages.
			require.NoError(t, l.Close())
			l, cleanup = setupWithOptions(t, opts)
			defer cleanup()
			defer l.Close()

			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			r, err = l.NewReaderUncommitted(ctx, 0)
			require.NoError(t, err)
			for _, ms := range msgs {
				buf := make([]byte, ms.Size())
				_, err := r.Read(buf)
				require.NoError(t, err)
				require.Equal(t, ms, commitlog.MessageSet(buf))
			}
		})
	}
}

func TestCommitLogRecoverHW(t *testing.T) {
	opts := commitlog.Options{
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

func BenchmarkCommitLog(b *testing.B) {
	var err error
	l, cleanup := setup(b)
	defer l.Close()
	defer cleanup()

	msgSet := msgSets[0]

	for i := 0; i < b.N; i++ {
		_, err = l.Append(msgSet)
		require.NoError(b, err)
	}
}

func TestTruncate(t *testing.T) {
	var err error
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	for i, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		require.Equal(t, int64(i), l.NewestOffset())
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), l.NewestOffset())
	require.Equal(t, 2, len(l.Segments()))

	err = l.Truncate(1)
	require.NoError(t, err)
	require.Equal(t, 1, len(l.Segments()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReaderUncommitted(ctx, 0)
	require.NoError(t, err)

	for _, m := range msgSets[:1] {
		p := make([]byte, m.Size())
		_, err = r.Read(p)
		require.NoError(t, err)

		ms := commitlog.MessageSet(p)
		require.Equal(t, m, ms)
	}
}

func TestTruncateNothing(t *testing.T) {
	var err error
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	for i, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		require.Equal(t, int64(i), l.NewestOffset())
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), l.NewestOffset())
	require.Equal(t, 2, len(l.Segments()))

	err = l.Truncate(2)
	require.NoError(t, err)
	require.Equal(t, 2, len(l.Segments()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReaderUncommitted(ctx, 0)
	require.NoError(t, err)

	for _, m := range msgSets {
		p := make([]byte, m.Size())
		_, err = r.Read(p)
		require.NoError(t, err)

		ms := commitlog.MessageSet(p)
		require.Equal(t, m, ms)
	}
}

func TestTruncateRemoveSegments(t *testing.T) {
	var err error
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 15,
		MaxLogBytes:     -1,
	})
	defer l.Close()
	defer cleanup()

	numMsgs := 10
	msgs := make([]commitlog.MessageSet, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = commitlog.NewMessageSet(
			uint64(i), commitlog.NewMessage([]byte(strconv.Itoa(i))),
		)
	}
	for _, ms := range msgs {
		_, err := l.Append(ms)
		require.NoError(t, err)
	}

	require.Equal(t, int64(9), l.NewestOffset())
	require.Equal(t, 5, len(l.Segments()))

	err = l.Truncate(4)
	require.NoError(t, err)
	require.Equal(t, 2, len(l.Segments()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReaderUncommitted(ctx, 0)
	require.NoError(t, err)

	for _, m := range msgs[:3] {
		p := make([]byte, m.Size())
		_, err = r.Read(p)
		require.NoError(t, err)

		ms := commitlog.MessageSet(p)
		require.Equal(t, m, ms)
	}
}

func TestTruncateReplaceContainingSegment(t *testing.T) {
	var err error
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 20,
		MaxLogBytes:     -1,
	})
	defer l.Close()
	defer cleanup()

	numMsgs := 5
	msgs := make([]commitlog.MessageSet, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = commitlog.NewMessageSet(
			uint64(i), commitlog.NewMessage([]byte(strconv.Itoa(i))),
		)
	}
	for _, ms := range msgs {
		_, err := l.Append(ms)
		require.NoError(t, err)
	}

	require.Equal(t, int64(4), l.NewestOffset())
	require.Equal(t, 3, len(l.Segments()))

	err = l.Truncate(1)
	require.NoError(t, err)
	require.Equal(t, 1, len(l.Segments()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReaderUncommitted(ctx, 0)
	require.NoError(t, err)

	for _, m := range msgs[:1] {
		p := make([]byte, m.Size())
		_, err = r.Read(p)
		require.NoError(t, err)

		ms := commitlog.MessageSet(p)
		require.Equal(t, m, ms)
	}
}

func TestOffsets(t *testing.T) {
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 20,
		MaxLogBytes:     -1,
	})
	defer l.Close()
	defer cleanup()
	require.Equal(t, int64(0), l.OldestOffset())
	require.Equal(t, int64(-1), l.NewestOffset())

	numMsgs := 5
	msgs := make([]commitlog.MessageSet, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = commitlog.NewMessageSet(
			uint64(i), commitlog.NewMessage([]byte(strconv.Itoa(i))),
		)
	}
	for _, ms := range msgs {
		_, err := l.Append(ms)
		require.NoError(t, err)
	}

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
	var err error
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		require.NoError(t, err)
	}
	segments := l.Segments()
	require.Equal(t, 2, len(segments))

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		require.NoError(t, err)
	}
	require.Equal(t, 2, len(l.Segments()))
	for i, s := range l.Segments() {
		require.NotEqual(t, s, segments[i])
	}
}

func check(t require.TestingT, got, want []byte) {
	if !bytes.Equal(got, want) {
		t.Errorf("got = %s, want %s", string(got), string(want))
	}
}

func setup(t require.TestingT) (*commitlog.CommitLog, func()) {
	opts := commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 6,
		MaxLogBytes:     30,
	}
	return setupWithOptions(t, opts)
}

func setupWithOptions(t require.TestingT, opts commitlog.Options) (*commitlog.CommitLog, func()) {
	l, err := commitlog.New(opts)
	require.NoError(t, err)
	return l, func() {
		remove(t, opts.Path)
	}
}

func tempDir(t require.TestingT) string {
	p, err := ioutil.TempDir("", "commitlogtest")
	if err != nil {
		require.NoError(t, err)
	}
	return p
}

func remove(t require.TestingT, path string) {
	os.RemoveAll(path)
	os.MkdirAll(path, 0755)
}
