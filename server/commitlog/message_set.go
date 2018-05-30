package commitlog

import (
	"bytes"
	"encoding/binary"

	"github.com/tylertreat/liftbridge/server/proto"
)

const (
	offsetPos       = 0
	sizePos         = 8
	msgSetHeaderLen = 12
)

type MessageSet []byte

func EntriesForMessageSet(baseOffset, basePos int64, ms []byte) []Entry {
	entries := []Entry{}
	if len(ms) <= msgSetHeaderLen {
		return entries
	}
	var n int64
	for len(ms) > 0 {
		var (
			relPos = n
			m      = MessageSet(ms)
			offset = m.Offset()
			size   = m.Size()
		)
		entries = append(entries, Entry{
			Offset:   offset,
			Position: basePos + relPos,
			Size:     size + msgSetHeaderLen,
		})
		n += msgSetHeaderLen + int64(size)
		ms = ms[n:]
	}
	return entries
}

func NewMessageSetFromProto(baseOffset, basePos int64, msgs []*proto.Message) (
	MessageSet, []Entry, error) {

	var (
		buf     = new(bytes.Buffer)
		entries = make([]Entry, len(msgs))
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
		if err := binary.Write(buf, proto.Encoding, uint32(len)); err != nil {
			return nil, nil, err
		}
		n += 4
		if _, err := buf.Write(data); err != nil {
			return nil, nil, err
		}
		n += len
		entries[i] = Entry{
			Offset:   offset,
			Position: basePos + relPos,
			Size:     len + msgSetHeaderLen,
		}
	}
	return buf.Bytes(), entries, nil
}

func (ms MessageSet) Offset() int64 {
	return int64(proto.Encoding.Uint64(ms[offsetPos : offsetPos+8]))
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
