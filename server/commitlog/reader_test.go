package commitlog_test

import (
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/tylertreat/liftbridge/server/commitlog"
	"github.com/tylertreat/liftbridge/server/proto"
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
			buf, _, err := commitlog.ConsumeMessageSet(r, headers)
			require.NoError(t, err)
			ms := &proto.MessageSet{}
			decoder := proto.NewDecoder(buf)
			ms.Decode(decoder)
			require.NoError(t, err)
			require.Equal(t, int64(idx), ms.Offset)
			require.Equal(t, msgs[idx], ms.Messages[0])
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
	buf, _, err := commitlog.ConsumeMessageSet(r, headers)
	require.NoError(t, err)
	ms := &proto.MessageSet{}
	decoder := proto.NewDecoder(buf)
	ms.Decode(decoder)
	require.NoError(t, err)
	require.Equal(t, msg, ms.Messages[0])

	msg = &proto.Message{Value: []byte("hello")}

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*proto.Message{msg})
		require.NoError(t, err)
	}()

	buf, _, err = commitlog.ConsumeMessageSet(r, headers)
	require.NoError(t, err)
	ms = &proto.MessageSet{}
	decoder = proto.NewDecoder(buf)
	ms.Decode(decoder)
	require.NoError(t, err)
	require.Equal(t, msg, ms.Messages[0])
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
			buf, _, err := commitlog.ConsumeMessageSet(r, headers)
			require.NoError(t, err)
			ms := &proto.MessageSet{}
			decoder := proto.NewDecoder(buf)
			ms.Decode(decoder)
			require.NoError(t, err)
			require.Equal(t, int64(idx), ms.Offset)
			require.Equal(t, msgs[idx], ms.Messages[0])
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
	buf, _, err := commitlog.ConsumeMessageSet(r, headers)
	require.NoError(t, err)
	ms := &proto.MessageSet{}
	decoder := proto.NewDecoder(buf)
	ms.Decode(decoder)
	require.NoError(t, err)
	require.Equal(t, msg, ms.Messages[0])
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
			for _, msg := range msgs {
				buf, _, err := commitlog.ConsumeMessageSet(r, headers)
				require.NoError(t, err)
				ms := &proto.MessageSet{}
				decoder := proto.NewDecoder(buf)
				ms.Decode(decoder)
				require.NoError(t, err)
				require.Equal(t, msg, ms.Messages[0])
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
			for _, msg := range msgs[:5] {
				buf, _, err := commitlog.ConsumeMessageSet(r, headers)
				require.NoError(t, err)
				ms := &proto.MessageSet{}
				decoder := proto.NewDecoder(buf)
				ms.Decode(decoder)
				require.NoError(t, err)
				require.Equal(t, msg, ms.Messages[0])
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
	for _, msg := range msgs {
		buf, _, err := commitlog.ConsumeMessageSet(r, headers)
		require.NoError(t, err)
		ms := &proto.MessageSet{}
		decoder := proto.NewDecoder(buf)
		ms.Decode(decoder)
		require.NoError(t, err)
		require.Equal(t, msg, ms.Messages[0])
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
	for _, msg := range msgs {
		buf, _, err := commitlog.ConsumeMessageSet(r, headers)
		if count < 5 {
			require.NoError(t, err)
			ms := &proto.MessageSet{}
			decoder := proto.NewDecoder(buf)
			ms.Decode(decoder)
			require.NoError(t, err)
			require.Equal(t, msg, ms.Messages[0])
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
	buf, _, err := commitlog.ConsumeMessageSet(r, headers)
	require.NoError(t, err)
	ms := &proto.MessageSet{}
	decoder := proto.NewDecoder(buf)
	ms.Decode(decoder)
	require.NoError(t, err)
	require.Equal(t, msg1, ms.Messages[0])
}
