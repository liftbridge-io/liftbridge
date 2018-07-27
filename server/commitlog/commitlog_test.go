package commitlog_test

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/liftbridge-io/liftbridge/server/commitlog"
	"github.com/liftbridge-io/liftbridge/server/proto"
)

var (
	msgs = []*proto.Message{
		&proto.Message{Value: []byte("one")},
		&proto.Message{Value: []byte("two")},
		&proto.Message{Value: []byte("three")},
		&proto.Message{Value: []byte("four")},
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
	r, err := l.NewReaderUncommitted(ctx, 0)
	require.NoError(t, err)

	headers := make([]byte, 12)
	for i, exp := range msgs {
		msg, offset, err := commitlog.ReadMessage(r, headers)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
		compareMessages(t, exp, msg)
	}
}

func TestCommitLogRecover(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			opts := commitlog.Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			}
			l, cleanup := setupWithOptions(t, opts)
			defer cleanup()

			// Append some messages.
			numMsgs := 10
			msgs := make([]*proto.Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &proto.Message{Value: []byte(strconv.Itoa(i))}
			}
			for _, msg := range msgs {
				_, err := l.Append([]*proto.Message{msg})
				require.NoError(t, err)
			}

			// Read them back as a sanity check.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := l.NewReaderUncommitted(ctx, 0)
			require.NoError(t, err)

			headers := make([]byte, 12)
			for i, exp := range msgs {
				msg, offset, err := commitlog.ReadMessage(r, headers)
				require.NoError(t, err)
				compareMessages(t, exp, msg)
				require.Equal(t, int64(i), offset)
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
			for i, exp := range msgs {
				msg, offset, err := commitlog.ReadMessage(r, headers)
				require.NoError(t, err)
				compareMessages(t, exp, msg)
				require.Equal(t, int64(i), offset)
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

	for i := 0; i < b.N; i++ {
		_, err = l.Append(msgs)
		require.NoError(b, err)
	}
}

func TestOffsets(t *testing.T) {
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 20,
	})
	defer l.Close()
	defer cleanup()
	require.Equal(t, int64(0), l.OldestOffset())
	require.Equal(t, int64(-1), l.NewestOffset())

	numMsgs := 5
	msgs := make([]*proto.Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &proto.Message{Value: []byte(strconv.Itoa(i))}
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
	var err error
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	_, err = l.Append(msgs)
	require.NoError(t, err)
	segments := l.Segments()
	require.Equal(t, 1, len(segments))

	_, err = l.Append(msgs)
	require.NoError(t, err)
	require.Equal(t, 1, len(l.Segments()))
	for i, s := range l.Segments() {
		require.NotEqual(t, s, segments[i])
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
