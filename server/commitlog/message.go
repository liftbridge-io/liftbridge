package commitlog

import "github.com/liftbridge-io/liftbridge/server/proto"

type Message []byte

func (m Message) Crc() int32 {
	return int32(proto.Encoding.Uint32(m))
}

func (m Message) MagicByte() int8 {
	return int8(m[4])
}

func (m Message) Attributes() int8 {
	return int8(m[5])
}

func (m Message) Timestamp() int64 {
	return int64(proto.Encoding.Uint64(m[6:]))
}

func (m Message) Key() []byte {
	start, end, size := m.keyOffsets()
	if size == -1 {
		return nil
	}
	return m[start+4 : end]
}

func (m Message) Value() []byte {
	start, end, size := m.valueOffsets()
	if size == -1 {
		return nil
	}
	return m[start+4 : end]
}

func (m Message) Headers() map[string][]byte {
	var (
		_, valueEnd, _ = m.valueOffsets()
		n              = valueEnd
		numHeaders     = proto.Encoding.Uint16(m[n:])
		headers        = make(map[string][]byte, numHeaders)
	)
	n += 2
	for i := uint16(0); i < numHeaders; i++ {
		keySize := proto.Encoding.Uint16(m[n:])
		n += 2
		key := string(m[n : n+int32(keySize)])
		n += int32(keySize)
		valueSize := proto.Encoding.Uint32(m[n:])
		n += 4
		value := m[n : n+int32(valueSize)]
		n += int32(valueSize)
		headers[key] = value
	}
	return headers
}

func (m Message) keyOffsets() (start, end, size int32) {
	start = 14
	size = int32(proto.Encoding.Uint32(m[start:]))
	end = start + 4
	if size != -1 {
		end += size
	}
	return
}

func (m Message) valueOffsets() (start, end, size int32) {
	_, keyEnd, _ := m.keyOffsets()
	start = keyEnd
	size = int32(proto.Encoding.Uint32(m[start:]))
	end = start + 4 + size
	return
}
