package commitlog

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

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
			l, cleanup := setupWithOptions(t, Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			})
			defer l.Close()
			defer cleanup()

			numMsgs := 10
			msgs := make([]*proto.Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &proto.Message{
					Value:       []byte(strconv.Itoa(i)),
					Timestamp:   int64(i),
					LeaderEpoch: 42,
				}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			idx := 4
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := l.NewReader(int64(idx), true)
			require.NoError(t, err)

			headers := make([]byte, 28)
			msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers)
			require.NoError(t, err)
			require.Equal(t, int64(idx), offset)
			require.Equal(t, int64(idx), timestamp)
			require.Equal(t, uint64(42), leaderEpoch)
			compareMessages(t, msgs[idx], msg)
		})
	}
}

func TestReaderUncommittedBlockCancel(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
	})
	defer l.Close()
	defer cleanup()

	msg := &proto.Message{Value: []byte("hi")}
	_, err := l.Append([]*proto.Message{msg})
	require.NoError(t, err)

	r, err := l.NewReader(0, true)
	require.NoError(t, err)

	headers := make([]byte, 28)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go cancel()
	_, _, _, _, err = r.ReadMessage(ctx, headers)
	require.Equal(t, io.EOF, errors.Cause(err))
}

func TestReaderUncommittedBlockForSegmentWrite(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer l.Close()
	defer cleanup()

	msg := &proto.Message{
		Value:       []byte("hi"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}
	_, err := l.Append([]*proto.Message{msg})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(0, true)
	require.NoError(t, err)
	headers := make([]byte, 28)
	m, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
	require.Equal(t, int64(1), timestamp)
	require.Equal(t, uint64(42), leaderEpoch)
	compareMessages(t, msg, m)

	msg = &proto.Message{
		Value:       []byte("hello"),
		Timestamp:   2,
		LeaderEpoch: 43,
	}

	done := make(chan struct{})

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*proto.Message{msg})
		require.NoError(t, err)
		close(done)
	}()

	m, offset, timestamp, leaderEpoch, err = r.ReadMessage(ctx, headers)
	require.NoError(t, err)
	require.Equal(t, int64(1), offset)
	require.Equal(t, int64(2), timestamp)
	require.Equal(t, uint64(43), leaderEpoch)
	compareMessages(t, msg, m)

	<-done
}

func TestReaderUncommittedReadError(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer cleanup()

	msg := &proto.Message{Value: []byte("hi")}
	_, err := l.Append([]*proto.Message{msg})
	require.NoError(t, err)

	r, err := l.NewReader(0, true)
	require.NoError(t, err)

	require.NoError(t, l.Close())

	p := make([]byte, 10)
	_, _, _, _, err = r.ReadMessage(context.Background(), p)
	require.Error(t, err)
}

func TestReaderCommittedStartOffset(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l, cleanup := setupWithOptions(t, Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			})
			defer l.Close()
			defer cleanup()

			numMsgs := 10
			msgs := make([]*proto.Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &proto.Message{
					Value:       []byte(strconv.Itoa(i)),
					Timestamp:   int64(i),
					LeaderEpoch: 42,
				}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			l.SetHighWatermark(4)
			idx := 2
			r, err := l.NewReader(int64(idx), false)
			require.NoError(t, err)

			headers := make([]byte, 28)
			msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
			require.NoError(t, err)
			require.Equal(t, int64(idx), offset)
			require.Equal(t, int64(idx), timestamp)
			require.Equal(t, uint64(42), leaderEpoch)
			compareMessages(t, msgs[idx], msg)
		})
	}
}

func TestReaderCommittedBlockCancel(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
	})
	defer l.Close()
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	r, err := l.NewReader(0, false)
	require.NoError(t, err)
	go cancel()
	headers := make([]byte, 28)
	_, _, _, _, err = r.ReadMessage(ctx, headers)
	require.Equal(t, io.EOF, errors.Cause(err))
}

func TestReaderCommittedReadError(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer cleanup()

	msg := &proto.Message{Value: []byte("hi")}
	_, err := l.Append([]*proto.Message{msg})
	require.NoError(t, err)
	msg = &proto.Message{Value: []byte("hi")}
	_, err = l.Append([]*proto.Message{msg})
	require.NoError(t, err)
	l.SetHighWatermark(0)

	r, err := l.NewReader(0, false)
	require.NoError(t, err)

	require.NoError(t, l.Close())

	headers := make([]byte, 28)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Error(t, err)
}

func TestReaderCommittedWaitOnEmptyLog(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
	})
	defer l.Close()
	defer cleanup()

	r, err := l.NewReader(0, false)
	require.NoError(t, err)

	msg := &proto.Message{
		Value:       []byte("hello"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*proto.Message{msg})
		require.NoError(t, err)
		l.SetHighWatermark(0)
	}()

	headers := make([]byte, 28)
	m, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
	require.Equal(t, int64(1), timestamp)
	require.Equal(t, uint64(42), leaderEpoch)
	compareMessages(t, msg, m)
}

func TestReaderCommittedRead(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l, cleanup := setupWithOptions(t, Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			})
			defer l.Close()
			defer cleanup()

			numMsgs := 10
			msgs := make([]*proto.Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &proto.Message{
					Value:       []byte(strconv.Itoa(i)),
					Timestamp:   int64(i),
					LeaderEpoch: 42,
				}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			l.SetHighWatermark(9)
			r, err := l.NewReader(0, false)
			require.NoError(t, err)

			headers := make([]byte, 28)
			for i, msg := range msgs {
				m, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
				require.NoError(t, err)
				require.Equal(t, int64(i), offset)
				require.Equal(t, int64(i), timestamp)
				require.Equal(t, uint64(42), leaderEpoch)
				compareMessages(t, msg, m)
			}
		})
	}
}

func TestReaderCommittedReadToHW(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l, cleanup := setupWithOptions(t, Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			})
			defer l.Close()
			defer cleanup()

			numMsgs := 10
			msgs := make([]*proto.Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &proto.Message{
					Value:       []byte(strconv.Itoa(i)),
					Timestamp:   int64(i),
					LeaderEpoch: 42,
				}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			l.SetHighWatermark(4)
			r, err := l.NewReader(0, false)
			require.NoError(t, err)

			headers := make([]byte, 28)
			for i, msg := range msgs[:5] {
				m, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
				require.NoError(t, err)
				require.Equal(t, int64(i), offset)
				require.Equal(t, int64(i), timestamp)
				require.Equal(t, uint64(42), leaderEpoch)
				compareMessages(t, msg, m)
			}
		})
	}
}

func TestReaderCommittedWaitForHW(t *testing.T) {
	var err error
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 30,
	})
	defer l.Close()
	defer cleanup()

	numMsgs := 10
	msgs := make([]*proto.Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &proto.Message{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   int64(i),
			LeaderEpoch: 42,
		}
	}
	_, err = l.Append(msgs)
	require.NoError(t, err)
	l.SetHighWatermark(4)
	r, err := l.NewReader(0, false)
	require.NoError(t, err)

	go func() {
		time.Sleep(5 * time.Millisecond)
		l.SetHighWatermark(9)
	}()

	headers := make([]byte, 28)
	for i, msg := range msgs {
		m, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
		require.Equal(t, int64(i), timestamp)
		require.Equal(t, uint64(42), leaderEpoch)
		compareMessages(t, msg, m)
	}
}

func TestReaderCommittedCancel(t *testing.T) {
	var err error
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 30,
	})
	defer l.Close()
	defer cleanup()

	numMsgs := 10
	msgs := make([]*proto.Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &proto.Message{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   int64(i),
			LeaderEpoch: 42,
		}
	}
	_, err = l.Append(msgs)
	require.NoError(t, err)
	l.SetHighWatermark(4)
	ctx, cancel := context.WithCancel(context.Background())
	r, err := l.NewReader(0, false)
	require.NoError(t, err)

	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	count := 0
	headers := make([]byte, 28)
	for i, msg := range msgs {
		m, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers)
		if count < 5 {
			require.NoError(t, err)
			require.Equal(t, int64(i), offset)
			require.Equal(t, int64(i), timestamp)
			require.Equal(t, uint64(42), leaderEpoch)
			compareMessages(t, msg, m)
			count++
		} else {
			require.Equal(t, io.EOF, errors.Cause(err))
		}
	}
	require.Equal(t, 5, count)
}

// Ensure ReadMessage waits for the next message when the offset exceeds the
// HW.
func TestReaderCommittedCapOffset(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer cleanup()

	msg1 := &proto.Message{
		Value:       []byte("hi"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}
	_, err := l.Append([]*proto.Message{msg1})
	require.NoError(t, err)
	msg2 := &proto.Message{
		Value:       []byte("hi"),
		Timestamp:   2,
		LeaderEpoch: 42,
	}
	_, err = l.Append([]*proto.Message{msg2})
	require.NoError(t, err)
	l.SetHighWatermark(0)

	r, err := l.NewReader(5, false)
	require.NoError(t, err)

	go l.SetHighWatermark(1)

	headers := make([]byte, 28)
	m, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
	require.Equal(t, int64(1), offset)
	require.Equal(t, int64(2), timestamp)
	require.Equal(t, uint64(42), leaderEpoch)
	compareMessages(t, msg2, m)
}

func compareMessages(t *testing.T, exp *proto.Message, act Message) {
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
