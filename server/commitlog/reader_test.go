package commitlog

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
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
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{
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

	msg := &Message{Value: []byte("hi")}
	_, err := l.Append([]*Message{msg})
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

	msg := &Message{
		Value:       []byte("hi"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}
	_, err := l.Append([]*Message{msg})
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

	msg = &Message{
		Value:       []byte("hello"),
		Timestamp:   2,
		LeaderEpoch: 43,
	}

	done := make(chan struct{})

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*Message{msg})
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

	msg := &Message{Value: []byte("hi")}
	_, err := l.Append([]*Message{msg})
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
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{
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

	msg := &Message{Value: []byte("hi")}
	_, err := l.Append([]*Message{msg})
	require.NoError(t, err)
	msg = &Message{Value: []byte("hi")}
	_, err = l.Append([]*Message{msg})
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

	msg := &Message{
		Value:       []byte("hello"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*Message{msg})
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

func TestReaderCommittedWaitOnEmptyLogWithHW(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1024,
	})
	defer l.Close()
	defer cleanup()

	// Simulate a log that had retention limits applied past a HW.
	l.SetHighWatermark(9)
	l.segments[0].BaseOffset = 10

	r, err := l.NewReader(0, false)
	require.NoError(t, err)

	msg := &Message{
		Value:       []byte("hello"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*Message{msg})
		require.NoError(t, err)
		l.SetHighWatermark(10)
	}()

	headers := make([]byte, 28)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
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
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{
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
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{
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
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{
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
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{
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

	msg1 := &Message{
		Value:       []byte("hi"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}
	_, err := l.Append([]*Message{msg1})
	require.NoError(t, err)
	msg2 := &Message{
		Value:       []byte("hi"),
		Timestamp:   2,
		LeaderEpoch: 42,
	}
	_, err = l.Append([]*Message{msg2})
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

// Ensure ReadMessage returns ErrCommitLogDeleted when the commit log is
// deleted.
func TestReaderLogDeleted(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
	})
	defer l.Close()
	defer cleanup()

	r, err := l.NewReader(0, false)
	require.NoError(t, err)

	go func() {
		require.NoError(t, l.Delete())
	}()

	headers := make([]byte, 28)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Equal(t, ErrCommitLogDeleted, err)
}

// Ensure ReadMessage returns ErrCommitLogClosed when the commit log is closed.
func TestReaderLogClosed(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
	})
	defer l.Close()
	defer cleanup()

	r, err := l.NewReader(0, false)
	require.NoError(t, err)

	go func() {
		require.NoError(t, l.Close())
	}()

	headers := make([]byte, 28)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Equal(t, ErrCommitLogClosed, err)
}

func compareMessages(t *testing.T, exp *Message, act SerializedMessage) {
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

// TestReverseReaderUncommitted tests that the ReverseReader reads messages
// in reverse order (newest to oldest) from an uncommitted log.
func TestReverseReaderUncommitted(t *testing.T) {
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
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{
					Value:       []byte(strconv.Itoa(i)),
					Timestamp:   int64(i),
					LeaderEpoch: 42,
				}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)

			// Create reverse reader starting at offset 9 (last message)
			r, err := l.NewReverseReader(9, true)
			require.NoError(t, err)

			headers := make([]byte, 28)
			// Read all messages in reverse order
			for i := numMsgs - 1; i >= 0; i-- {
				msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
				require.NoError(t, err)
				require.Equal(t, int64(i), offset)
				require.Equal(t, int64(i), timestamp)
				require.Equal(t, uint64(42), leaderEpoch)
				compareMessages(t, msgs[i], msg)
			}

			// Next read should return EOF
			_, _, _, _, err = r.ReadMessage(context.Background(), headers)
			require.Equal(t, io.EOF, err)
		})
	}
}

// TestReverseReaderCommitted tests that the ReverseReader reads only committed
// messages in reverse order.
func TestReverseReaderCommitted(t *testing.T) {
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
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{
					Value:       []byte(strconv.Itoa(i)),
					Timestamp:   int64(i),
					LeaderEpoch: 42,
				}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			l.SetHighWatermark(7)

			// Create reverse reader starting at offset 9 (should cap to HW=7)
			r, err := l.NewReverseReader(9, false)
			require.NoError(t, err)

			headers := make([]byte, 28)
			// Read messages in reverse order from HW (7) down to 0
			for i := 7; i >= 0; i-- {
				msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
				require.NoError(t, err)
				require.Equal(t, int64(i), offset)
				require.Equal(t, int64(i), timestamp)
				require.Equal(t, uint64(42), leaderEpoch)
				compareMessages(t, msgs[i], msg)
			}

			// Next read should return EOF
			_, _, _, _, err = r.ReadMessage(context.Background(), headers)
			require.Equal(t, io.EOF, err)
		})
	}
}

// TestReverseReaderWithStopOffset tests that the ReverseReader stops at the
// specified stop offset.
func TestReverseReaderWithStopOffset(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer l.Close()
	defer cleanup()

	numMsgs := 10
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   int64(i),
			LeaderEpoch: 42,
		}
	}
	_, err := l.Append(msgs)
	require.NoError(t, err)

	r, err := l.NewReverseReader(9, true)
	require.NoError(t, err)
	r.SetStopOffset(5)

	headers := make([]byte, 28)
	// Read messages from 9 down to 5 (inclusive)
	for i := 9; i >= 5; i-- {
		msg, offset, _, _, err := r.ReadMessage(context.Background(), headers)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
		compareMessages(t, msgs[i], msg)
	}

	// Next read should return EOF because we've reached stop offset
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Equal(t, io.EOF, err)
}

// TestReverseReaderFromEnd tests creating a ReverseReader from the end of log.
func TestReverseReaderFromEnd(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer l.Close()
	defer cleanup()

	numMsgs := 5
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   int64(i),
			LeaderEpoch: 42,
		}
	}
	_, err := l.Append(msgs)
	require.NoError(t, err)

	r, err := l.NewReverseReaderFromEnd(true)
	require.NoError(t, err)

	headers := make([]byte, 28)
	// Read all messages in reverse order starting from end
	for i := numMsgs - 1; i >= 0; i-- {
		msg, offset, _, _, err := r.ReadMessage(context.Background(), headers)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
		compareMessages(t, msgs[i], msg)
	}
}

// TestReverseReaderEmptyLog tests that NewReverseReader returns error for
// empty log.
func TestReverseReaderEmptyLog(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer l.Close()
	defer cleanup()

	_, err := l.NewReverseReader(0, true)
	require.Equal(t, ErrSegmentNotFound, err)

	_, err = l.NewReverseReaderFromEnd(true)
	require.Equal(t, ErrSegmentNotFound, err)
}

// TestReverseReaderContextCancel tests that ReverseReader respects context
// cancellation.
func TestReverseReaderContextCancel(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer l.Close()
	defer cleanup()

	msg := &Message{Value: []byte("hi")}
	_, err := l.Append([]*Message{msg})
	require.NoError(t, err)

	r, err := l.NewReverseReader(0, true)
	require.NoError(t, err)

	headers := make([]byte, 28)
	// First read should succeed
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)

	// Subsequent read with cancelled context should return EOF
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, _, err = r.ReadMessage(ctx, headers)
	require.Equal(t, io.EOF, err)
}

// TestReverseReaderLogDeleted tests that ReverseReader returns error when log
// is deleted.
func TestReverseReaderLogDeleted(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer cleanup()

	msgs := make([]*Message, 5)
	for i := 0; i < 5; i++ {
		msgs[i] = &Message{Value: []byte(strconv.Itoa(i))}
	}
	_, err := l.Append(msgs)
	require.NoError(t, err)

	r, err := l.NewReverseReader(4, true)
	require.NoError(t, err)

	// Read first message
	headers := make([]byte, 28)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)

	// Delete the log
	require.NoError(t, l.Delete())

	// Next read should return ErrCommitLogDeleted
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Equal(t, ErrCommitLogDeleted, err)
}

// TestReverseReaderLogClosed tests that ReverseReader returns error when log
// is closed.
func TestReverseReaderLogClosed(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer cleanup()

	msgs := make([]*Message, 5)
	for i := 0; i < 5; i++ {
		msgs[i] = &Message{Value: []byte(strconv.Itoa(i))}
	}
	_, err := l.Append(msgs)
	require.NoError(t, err)

	r, err := l.NewReverseReader(4, true)
	require.NoError(t, err)

	// Read first message
	headers := make([]byte, 28)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)

	// Close the log
	require.NoError(t, l.Close())

	// Next read should return ErrCommitLogClosed
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Equal(t, ErrCommitLogClosed, err)
}
