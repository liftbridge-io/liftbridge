package commitlog_test

import (
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/liftbridge-io/liftbridge/server/commitlog"
	"github.com/liftbridge-io/liftbridge/server/proto"
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
			msgs := make([]*proto.Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &proto.Message{Value: []byte(strconv.Itoa(i))}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			idx := 4
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := l.NewReaderUncommitted(ctx, int64(idx))
			require.NoError(t, err)

			headers := make([]byte, 12)
			msg, offset, err := commitlog.ReadMessage(r, headers)
			require.NoError(t, err)
			require.Equal(t, int64(idx), offset)
			compareMessages(t, msgs[idx], msg)
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

	msg := &proto.Message{Value: []byte("hi")}
	_, err := l.Append([]*proto.Message{msg})
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

	msg := &proto.Message{Value: []byte("hi")}
	_, err := l.Append([]*proto.Message{msg})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReaderUncommitted(ctx, 0)
	require.NoError(t, err)
	headers := make([]byte, 12)
	m, offset, err := commitlog.ReadMessage(r, headers)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
	compareMessages(t, msg, m)

	msg = &proto.Message{Value: []byte("hello")}

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*proto.Message{msg})
		require.NoError(t, err)
	}()

	m, offset, err = commitlog.ReadMessage(r, headers)
	require.NoError(t, err)
	require.Equal(t, int64(1), offset)
	compareMessages(t, msg, m)
}

func TestReaderUncommittedReadError(t *testing.T) {
	l, cleanup := setupWithOptions(t, commitlog.Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
		MaxLogBytes:     -1,
	})
	defer cleanup()

	msg := &proto.Message{Value: []byte("hi")}
	_, err := l.Append([]*proto.Message{msg})
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
			msgs := make([]*proto.Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &proto.Message{Value: []byte(strconv.Itoa(i))}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			l.SetHighWatermark(4)
			idx := 2
			r, err := l.NewReaderCommitted(context.Background(), int64(idx))
			require.NoError(t, err)

			headers := make([]byte, 12)
			msg, offset, err := commitlog.ReadMessage(r, headers)
			require.NoError(t, err)
			require.Equal(t, int64(idx), offset)
			compareMessages(t, msgs[idx], msg)
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

	msg := &proto.Message{Value: []byte("hi")}
	_, err := l.Append([]*proto.Message{msg})
	require.NoError(t, err)
	msg = &proto.Message{Value: []byte("hi")}
	_, err = l.Append([]*proto.Message{msg})
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

	msg := &proto.Message{Value: []byte("hello")}

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*proto.Message{msg})
		require.NoError(t, err)
		l.SetHighWatermark(0)
	}()

	headers := make([]byte, 12)
	m, offset, err := commitlog.ReadMessage(r, headers)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
	compareMessages(t, msg, m)
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
			msgs := make([]*proto.Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &proto.Message{Value: []byte(strconv.Itoa(i))}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			l.SetHighWatermark(9)
			r, err := l.NewReaderCommitted(context.Background(), 0)
			require.NoError(t, err)

			headers := make([]byte, 12)
			for i, msg := range msgs {
				m, offset, err := commitlog.ReadMessage(r, headers)
				require.NoError(t, err)
				require.Equal(t, int64(i), offset)
				compareMessages(t, msg, m)
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
			msgs := make([]*proto.Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &proto.Message{Value: []byte(strconv.Itoa(i))}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			l.SetHighWatermark(4)
			r, err := l.NewReaderCommitted(context.Background(), 0)
			require.NoError(t, err)

			headers := make([]byte, 12)
			for i, msg := range msgs[:5] {
				m, offset, err := commitlog.ReadMessage(r, headers)
				require.NoError(t, err)
				require.Equal(t, int64(i), offset)
				compareMessages(t, msg, m)
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
	msgs := make([]*proto.Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &proto.Message{Value: []byte(strconv.Itoa(i))}
	}
	_, err = l.Append(msgs)
	require.NoError(t, err)
	l.SetHighWatermark(4)
	r, err := l.NewReaderCommitted(context.Background(), 0)
	require.NoError(t, err)

	go func() {
		time.Sleep(5 * time.Millisecond)
		l.SetHighWatermark(9)
	}()

	headers := make([]byte, 12)
	for i, msg := range msgs {
		m, offset, err := commitlog.ReadMessage(r, headers)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
		compareMessages(t, msg, m)
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
	msgs := make([]*proto.Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &proto.Message{Value: []byte(strconv.Itoa(i))}
	}
	_, err = l.Append(msgs)
	require.NoError(t, err)
	l.SetHighWatermark(4)
	ctx, cancel := context.WithCancel(context.Background())
	r, err := l.NewReaderCommitted(ctx, 0)
	require.NoError(t, err)

	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	count := 0
	headers := make([]byte, 12)
	for i, msg := range msgs {
		m, offset, err := commitlog.ReadMessage(r, headers)
		if count < 5 {
			require.NoError(t, err)
			require.Equal(t, int64(i), offset)
			compareMessages(t, msg, m)
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

	msg1 := &proto.Message{Value: []byte("hi")}
	_, err := l.Append([]*proto.Message{msg1})
	require.NoError(t, err)
	msg2 := &proto.Message{Value: []byte("hi")}
	_, err = l.Append([]*proto.Message{msg2})
	require.NoError(t, err)
	l.SetHighWatermark(0)

	r, err := l.NewReaderCommitted(context.Background(), 1)
	require.NoError(t, err)

	headers := make([]byte, 12)
	m, offset, err := commitlog.ReadMessage(r, headers)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
	compareMessages(t, msg1, m)
}

func compareMessages(t *testing.T, exp *proto.Message, act commitlog.Message) {
	// TODO: check timestamp
	require.Equal(t, exp.MagicByte, act.MagicByte())
	require.Equal(t, exp.Attributes, act.Attributes())
	require.Equal(t, exp.Key, act.Key())
	require.Equal(t, exp.Value, act.Value())
	if exp.Headers == nil || len(exp.Headers) == 0 {
		require.Equal(t, map[string][]byte{}, act.Headers())
	} else {
		require.Equal(t, exp.Headers, act.Headers())
	}
}
