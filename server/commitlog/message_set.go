package commitlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/pkg/errors"
)

const (
	offsetPos       = 0
	timestampPos    = 8
	leaderEpochPos  = 16
	sizePos         = 24
	msgSetHeaderLen = 28
)

type messageSet []byte

func entriesForMessageSet(basePos int64, ms []byte) []*entry {
	entries := []*entry{}
	if len(ms) <= msgSetHeaderLen {
		return entries
	}
	var n int64
	for len(ms) > 0 {
		var (
			relPos      = n
			m           = messageSet(ms)
			offset      = m.Offset()
			timestamp   = m.Timestamp()
			leaderEpoch = m.LeaderEpoch()
			size        = m.Size()
		)
		entries = append(entries, &entry{
			Offset:      offset,
			Timestamp:   timestamp,
			LeaderEpoch: leaderEpoch,
			Position:    basePos + relPos,
			Size:        size + msgSetHeaderLen,
		})
		n += msgSetHeaderLen + int64(size)
		ms = ms[msgSetHeaderLen+size:]
	}
	return entries
}

func newMessageSetFromProto(baseOffset, basePos int64, msgs []*Message) (
	messageSet, []*entry, error) {

	var (
		buf     = new(bytes.Buffer)
		entries = make([]*entry, len(msgs))
		n       int32
	)
	for i, m := range msgs {
		data, err := encode(m)
		if err != nil {
			panic(err)
		}
		var (
			len    = int32(len(data))
			relPos = int64(n)
			offset = int64(i) + baseOffset
		)
		if err := binary.Write(buf, encoding, uint64(offset)); err != nil {
			return nil, nil, err
		}
		n += 8
		if err := binary.Write(buf, encoding, uint64(m.Timestamp)); err != nil {
			return nil, nil, err
		}
		n += 8
		if err := binary.Write(buf, encoding, m.LeaderEpoch); err != nil {
			return nil, nil, err
		}
		n += 8
		if err := binary.Write(buf, encoding, uint32(len)); err != nil {
			return nil, nil, err
		}
		n += 4
		if _, err := buf.Write(data); err != nil {
			return nil, nil, err
		}
		n += len
		entries[i] = &entry{
			Offset:      offset,
			Timestamp:   m.Timestamp,
			LeaderEpoch: m.LeaderEpoch,
			Position:    basePos + relPos,
			Size:        len + msgSetHeaderLen,
		}
	}
	return buf.Bytes(), entries, nil
}

// readMessage reads a single message from the reader or blocks until one is
// available. It returns the Message in addition to its offset, timestamp, and
// leader epoch. This may return uncommitted messages if the reader was created
// with the uncommitted flag set to true.
func readMessage(ctx context.Context, reader contextReader, headersBuf []byte, noBlocking chan struct{}) (SerializedMessage, int64, int64, uint64, error) {
	if _, err := reader.Read(ctx, headersBuf, noBlocking); err != nil {
		return nil, 0, 0, 0, errors.Wrap(err, "failed to read message headers")
	}
	var (
		offset      = int64(encoding.Uint64(headersBuf[offsetPos:]))
		timestamp   = int64(encoding.Uint64(headersBuf[timestampPos:]))
		leaderEpoch = encoding.Uint64(headersBuf[leaderEpochPos:])
		size        = encoding.Uint32(headersBuf[sizePos:])
		buf         = make([]byte, int(size))
	)
	if _, err := reader.Read(ctx, buf, noBlocking); err != nil {
		return nil, 0, 0, 0, errors.Wrap(err, "failed to ready message payload")
	}
	m := SerializedMessage(buf)
	// Check the CRC on the message.
	crc := m.Crc()
	if c := crc32.Checksum(m[4:], crc32cTable); crc != c {
		// If the CRC doesn't match, data on disk is corrupted which means the
		// server is in an unrecoverable state.
		panic(fmt.Errorf("Read corrupted data, expected CRC: 0x%08x, got: 0x%08x", crc, c))
	}
	return m, offset, timestamp, leaderEpoch, nil
}

func (ms messageSet) Offset() int64 {
	return int64(encoding.Uint64(ms[offsetPos : offsetPos+8]))
}

func (ms messageSet) Timestamp() int64 {
	return int64(encoding.Uint64(ms[timestampPos : timestampPos+8]))
}

func (ms messageSet) LeaderEpoch() uint64 {
	return encoding.Uint64(ms[leaderEpochPos : leaderEpochPos+8])
}

func (ms messageSet) Size() int32 {
	return int32(encoding.Uint32(ms[sizePos : sizePos+4]))
}

func (ms messageSet) Message() SerializedMessage {
	if len(ms) <= msgSetHeaderLen {
		return nil
	}
	size := ms.Size()
	return SerializedMessage(ms[msgSetHeaderLen : msgSetHeaderLen+size])
}
