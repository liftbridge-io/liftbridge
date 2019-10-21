package commitlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/pkg/errors"

	"github.com/liftbridge-io/liftbridge/server/proto"
)

const (
	offsetPos       = 0
	timestampPos    = 8
	leaderEpochPos  = 16
	sizePos         = 24
	msgSetHeaderLen = 28
)

type MessageSet []byte

func EntriesForMessageSet(basePos int64, ms []byte) []*Entry {
	entries := []*Entry{}
	if len(ms) <= msgSetHeaderLen {
		return entries
	}
	var n int64
	for len(ms) > 0 {
		var (
			relPos      = n
			m           = MessageSet(ms)
			offset      = m.Offset()
			timestamp   = m.Timestamp()
			leaderEpoch = m.LeaderEpoch()
			size        = m.Size()
		)
		entries = append(entries, &Entry{
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

func NewMessageSetFromProto(baseOffset, basePos int64, msgs []*proto.Message) (
	MessageSet, []*Entry, error) {

	var (
		buf     = new(bytes.Buffer)
		entries = make([]*Entry, len(msgs))
		n       int32
	)
	for i, m := range msgs {
		data, err := proto.Encode(m)
		if err != nil {
			panic(err)
		}
		var (
			len    = int32(len(data))
			relPos = int64(n)
			offset = int64(i) + baseOffset
		)
		if err := binary.Write(buf, proto.Encoding, uint64(offset)); err != nil {
			return nil, nil, err
		}
		n += 8
		if err := binary.Write(buf, proto.Encoding, uint64(m.Timestamp)); err != nil {
			return nil, nil, err
		}
		n += 8
		if err := binary.Write(buf, proto.Encoding, m.LeaderEpoch); err != nil {
			return nil, nil, err
		}
		n += 8
		if err := binary.Write(buf, proto.Encoding, uint32(len)); err != nil {
			return nil, nil, err
		}
		n += 4
		if _, err := buf.Write(data); err != nil {
			return nil, nil, err
		}
		n += len
		entries[i] = &Entry{
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
func readMessage(ctx context.Context, reader contextReader, headersBuf []byte) (Message, int64, int64, uint64, error) {
	if _, err := reader.Read(ctx, headersBuf); err != nil {
		return nil, 0, 0, 0, errors.Wrap(err, "failed to read message headers")
	}
	var (
		offset      = int64(proto.Encoding.Uint64(headersBuf[offsetPos:]))
		timestamp   = int64(proto.Encoding.Uint64(headersBuf[timestampPos:]))
		leaderEpoch = proto.Encoding.Uint64(headersBuf[leaderEpochPos:])
		size        = proto.Encoding.Uint32(headersBuf[sizePos:])
		buf         = make([]byte, int(size))
	)
	if _, err := reader.Read(ctx, buf); err != nil {
		return nil, 0, 0, 0, errors.Wrap(err, "failed to ready message payload")
	}
	m := Message(buf)
	// Check the CRC on the message.
	crc := m.Crc()
	if c := crc32.ChecksumIEEE(m[4:]); crc != c {
		// If the CRC doesn't match, data on disk is corrupted which means the
		// server is in an unrecoverable state.
		panic(fmt.Errorf("Read corrupted data, expected CRC: 0x%08x, got: 0x%08x", crc, c))
	}
	return m, offset, timestamp, leaderEpoch, nil
}

func (ms MessageSet) Offset() int64 {
	return int64(proto.Encoding.Uint64(ms[offsetPos : offsetPos+8]))
}

func (ms MessageSet) Timestamp() int64 {
	return int64(proto.Encoding.Uint64(ms[timestampPos : timestampPos+8]))
}

func (ms MessageSet) LeaderEpoch() uint64 {
	return proto.Encoding.Uint64(ms[leaderEpochPos : leaderEpochPos+8])
}

func (ms MessageSet) Size() int32 {
	return int32(proto.Encoding.Uint32(ms[sizePos : sizePos+4]))
}

func (ms MessageSet) Message() Message {
	if len(ms) <= msgSetHeaderLen {
		return nil
	}
	size := ms.Size()
	return Message(ms[msgSetHeaderLen : msgSetHeaderLen+size])
}
