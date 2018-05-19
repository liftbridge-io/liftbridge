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
	path string
)

func init() {
	p, err := ioutil.TempDir("", "commitlogtest")
	if err != nil {
		panic(err)
	}
	path = p
}

func TestNewCommitLog(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

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
			l := setupWithOptions(t, commitlog.Options{
				Path:            path,
				MaxSegmentBytes: test.segmentSize,
				MaxLogBytes:     -1,
			})
			defer cleanup(t)

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
			l = setupWithOptions(t, commitlog.Options{
				Path:            path,
				MaxSegmentBytes: test.segmentSize,
				MaxLogBytes:     -1,
			})

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
	l := setup(t)
	defer cleanup(t)
	l.SetHighWatermark(100)
	require.Equal(t, int64(100), l.HighWatermark())
	require.NoError(t, l.Close())
	l = setup(t)
	require.Equal(t, int64(100), l.HighWatermark())
}

func BenchmarkCommitLog(b *testing.B) {
	var err error
	l := setup(b)
	defer cleanup(b)

	msgSet := msgSets[0]

	for i := 0; i < b.N; i++ {
		_, err = l.Append(msgSet)
		require.NoError(b, err)
	}
}

func TestTruncate(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	for i, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		require.Equal(t, int64(i), l.NewestOffset())
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), l.NewestOffset())
	require.Equal(t, 2, len(l.Segments()))

	err = l.Truncate(int64(1))
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

func TestCleaner(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

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

func setup(t require.TestingT) *commitlog.CommitLog {
	opts := commitlog.Options{
		Path:            path,
		MaxSegmentBytes: 6,
		MaxLogBytes:     30,
	}
	return setupWithOptions(t, opts)
}

func setupWithOptions(t require.TestingT, opts commitlog.Options) *commitlog.CommitLog {
	l, err := commitlog.New(opts)
	require.NoError(t, err)
	return l
}

func cleanup(t require.TestingT) {
	os.RemoveAll(path)
	os.MkdirAll(path, 0755)
}
