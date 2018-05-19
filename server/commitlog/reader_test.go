package commitlog_test

import (
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/tylertreat/liftbridge/server/commitlog"
)

var segmentSizeTests = []struct {
	name        string
	segmentSize int64
}{
	{"6", 6},
	{"60", 60},
	{"600", 600},
	{"6000", 6000},
}

func TestReaderUncommittedStartOffset(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l := setupWithOptions(t, commitlog.Options{
				Path:            path,
				MaxSegmentBytes: test.segmentSize,
				MaxLogBytes:     -1,
			})
			defer cleanup(t)

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
			idx := 4
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := l.NewReaderUncommitted(ctx, int64(idx))
			require.NoError(t, err)

			p := make([]byte, msgs[idx].Size())
			read, err := r.Read(p)
			require.NoError(t, err)
			require.Equal(t, msgs[idx].Size(), int32(read))
			act := commitlog.MessageSet(p)
			require.Equal(t, int64(idx), act.Offset())
		})
	}
}

func TestReaderUncommittedBlockCancel(t *testing.T) {
	l := setupWithOptions(t, commitlog.Options{
		Path:            path,
		MaxSegmentBytes: 10,
		MaxLogBytes:     -1,
	})
	defer cleanup(t)

	ms := commitlog.NewMessageSet(0, commitlog.NewMessage([]byte("hi")))
	_, err := l.Append(ms)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	r, err := l.NewReaderUncommitted(ctx, 0)
	require.NoError(t, err)
	p := make([]byte, 100)
	go cancel()
	_, err = r.Read(p)
	require.Equal(t, io.EOF, err)
}

func TestReaderUncommittedBlockForSegmentWrite(t *testing.T) {
	l := setupWithOptions(t, commitlog.Options{
		Path:            path,
		MaxSegmentBytes: 100,
		MaxLogBytes:     -1,
	})
	defer cleanup(t)

	ms := commitlog.NewMessageSet(0, commitlog.NewMessage([]byte("hi")))
	_, err := l.Append(ms)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReaderUncommitted(ctx, 0)
	require.NoError(t, err)
	p := make([]byte, ms.Size())
	_, err = r.Read(p)
	require.NoError(t, err)
	require.Equal(t, ms, commitlog.MessageSet(p))

	ms = commitlog.NewMessageSet(1, commitlog.NewMessage([]byte("hello")))
	p = make([]byte, ms.Size())

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append(ms)
		require.NoError(t, err)
	}()

	_, err = r.Read(p)
	require.NoError(t, err)
	require.Equal(t, ms, commitlog.MessageSet(p))
}

func TestReaderCommittedStartOffset(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l := setupWithOptions(t, commitlog.Options{
				Path:            path,
				MaxSegmentBytes: test.segmentSize,
				MaxLogBytes:     -1,
			})
			defer cleanup(t)

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
			l.SetHighWatermark(4)
			idx := 2
			r, err := l.NewReaderCommitted(context.Background(), int64(idx))
			require.NoError(t, err)

			p := make([]byte, msgs[idx].Size())
			read, err := r.Read(p)
			require.NoError(t, err)
			require.Equal(t, msgs[idx].Size(), int32(read))
			act := commitlog.MessageSet(p)
			require.Equal(t, int64(idx), act.Offset())
		})
	}
}

func TestReaderCommittedBlockCancel(t *testing.T) {
	l := setupWithOptions(t, commitlog.Options{
		Path:            path,
		MaxSegmentBytes: 10,
		MaxLogBytes:     -1,
	})
	defer cleanup(t)

	ctx, cancel := context.WithCancel(context.Background())
	r, err := l.NewReaderCommitted(ctx, 0)
	require.NoError(t, err)
	p := make([]byte, 100)
	go cancel()
	_, err = r.Read(p)
	require.Equal(t, io.EOF, err)
}
