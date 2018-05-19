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
			l, cleanup := setupWithOptions(t, commitlog.Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
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
			idx := 4
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := l.NewReaderUncommitted(ctx, int64(idx))
			require.NoError(t, err)

			p := make([]byte, msgs[idx].Size())
			read, err := r.Read(p)
			require.NoError(t, err)
			require.Equal(t, msgs[idx].Size(), int32(read))
			require.Equal(t, msgs[idx], commitlog.MessageSet(p))
		})
	}
}

func TestReaderUncommittedBlockCancel(t *testing.T) {
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
		MaxLogBytes:     -1,
	})
	defer l.Close()
	defer cleanup()

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
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
		MaxLogBytes:     -1,
	})
	defer l.Close()
	defer cleanup()

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

func TestReaderUncommittedReadError(t *testing.T) {
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
		MaxLogBytes:     -1,
	})
	defer cleanup()

	ms := commitlog.NewMessageSet(0, commitlog.NewMessage([]byte("hi")))
	_, err := l.Append(ms)
	require.NoError(t, err)

	r, err := l.NewReaderUncommitted(context.Background(), 0)
	require.NoError(t, err)

	require.NoError(t, l.Close())

	p := make([]byte, 10)
	_, err = r.Read(p)
	require.Error(t, err)
}

func TestReaderCommittedStartOffset(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l, cleanup := setupWithOptions(t, commitlog.Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
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
			l.SetHighWatermark(4)
			idx := 2
			r, err := l.NewReaderCommitted(context.Background(), int64(idx))
			require.NoError(t, err)

			p := make([]byte, msgs[idx].Size())
			read, err := r.Read(p)
			require.NoError(t, err)
			require.Equal(t, msgs[idx].Size(), int32(read))
			require.Equal(t, msgs[idx], commitlog.MessageSet(p))
		})
	}
}

func TestReaderCommittedBlockCancel(t *testing.T) {
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
		MaxLogBytes:     -1,
	})
	defer l.Close()
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	r, err := l.NewReaderCommitted(ctx, 0)
	require.NoError(t, err)
	p := make([]byte, 100)
	go cancel()
	_, err = r.Read(p)
	require.Equal(t, io.EOF, err)
}

func TestReaderCommittedReadError(t *testing.T) {
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
		MaxLogBytes:     -1,
	})
	defer cleanup()

	ms := commitlog.NewMessageSet(0, commitlog.NewMessage([]byte("hi")))
	_, err := l.Append(ms)
	require.NoError(t, err)
	ms = commitlog.NewMessageSet(1, commitlog.NewMessage([]byte("hi")))
	_, err = l.Append(ms)
	require.NoError(t, err)
	l.SetHighWatermark(0)

	r, err := l.NewReaderCommitted(context.Background(), 0)
	require.NoError(t, err)

	require.NoError(t, l.Close())

	p := make([]byte, 10)
	_, err = r.Read(p)
	require.Error(t, err)
}

func TestReaderCommittedWaitOnEmptyLog(t *testing.T) {
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
		MaxLogBytes:     -1,
	})
	defer l.Close()
	defer cleanup()

	r, err := l.NewReaderCommitted(context.Background(), 0)
	require.NoError(t, err)

	ms := commitlog.NewMessageSet(0, commitlog.NewMessage([]byte("hello")))
	p := make([]byte, ms.Size())

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append(ms)
		require.NoError(t, err)
		l.SetHighWatermark(0)
	}()

	_, err = r.Read(p)
	require.NoError(t, err)
	require.Equal(t, ms, commitlog.MessageSet(p))
}

func TestReaderCommittedRead(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l, cleanup := setupWithOptions(t, commitlog.Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
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
			l.SetHighWatermark(9)
			r, err := l.NewReaderCommitted(context.Background(), 0)
			require.NoError(t, err)

			for _, ms := range msgs {
				p := make([]byte, ms.Size())
				read, err := r.Read(p)
				require.NoError(t, err)
				require.Equal(t, ms.Size(), int32(read))
				require.Equal(t, ms, commitlog.MessageSet(p))
			}
		})
	}
}

func TestReaderCommittedReadToHW(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l, cleanup := setupWithOptions(t, commitlog.Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
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
			l.SetHighWatermark(4)
			r, err := l.NewReaderCommitted(context.Background(), 0)
			require.NoError(t, err)

			for _, ms := range msgs[:5] {
				p := make([]byte, ms.Size())
				read, err := r.Read(p)
				require.NoError(t, err)
				require.Equal(t, ms.Size(), int32(read))
				require.Equal(t, ms, commitlog.MessageSet(p))
			}
		})
	}
}

func TestReaderCommittedWaitForHW(t *testing.T) {
	var err error
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 30,
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
	l.SetHighWatermark(4)
	r, err := l.NewReaderCommitted(context.Background(), 0)
	require.NoError(t, err)

	go func() {
		time.Sleep(5 * time.Millisecond)
		l.SetHighWatermark(9)
	}()

	for _, ms := range msgs {
		p := make([]byte, ms.Size())
		read, err := r.Read(p)
		require.NoError(t, err)
		require.Equal(t, ms.Size(), int32(read))
		require.Equal(t, ms, commitlog.MessageSet(p))
	}
}

func TestReaderCommittedCancel(t *testing.T) {
	var err error
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 30,
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
	l.SetHighWatermark(4)
	ctx, cancel := context.WithCancel(context.Background())
	r, err := l.NewReaderCommitted(ctx, 0)
	require.NoError(t, err)

	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	count := 0
	for _, ms := range msgs {
		p := make([]byte, ms.Size())
		read, err := r.Read(p)
		if count < 5 {
			require.NoError(t, err)
			require.Equal(t, ms.Size(), int32(read))
			require.Equal(t, ms, commitlog.MessageSet(p))
			count++
		} else {
			require.Equal(t, io.EOF, err)
		}
	}
	require.Equal(t, 5, count)
}

func TestReaderCommittedCapOffset(t *testing.T) {
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
		MaxLogBytes:     -1,
	})
	defer cleanup()

	ms1 := commitlog.NewMessageSet(0, commitlog.NewMessage([]byte("hi")))
	_, err := l.Append(ms1)
	require.NoError(t, err)
	ms2 := commitlog.NewMessageSet(1, commitlog.NewMessage([]byte("hi")))
	_, err = l.Append(ms2)
	require.NoError(t, err)
	l.SetHighWatermark(0)

	r, err := l.NewReaderCommitted(context.Background(), 1)
	require.NoError(t, err)

	p := make([]byte, ms1.Size())
	read, err := r.Read(p)
	require.NoError(t, err)
	require.Equal(t, ms1.Size(), int32(read))
	require.Equal(t, ms1, commitlog.MessageSet(p))
}
