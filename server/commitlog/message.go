package commitlog

import (
	"errors"
	"hash/crc32"

	client "github.com/liftbridge-io/liftbridge-api/v2/go"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Message is the object that gets serialized and written to the log.
type Message struct {
	Crc        int32
	MagicByte  int8
	Attributes int8
	Key        []byte
	Value      []byte
	Headers    map[string][]byte

	// Transient fields
	Timestamp     int64
	LeaderEpoch   uint64
	AckInbox      string
	CorrelationID string
	AckPolicy     client.AckPolicy
	Offset        int64
}

// Encode the Message into the packetEncoder.
func (m *Message) Encode(e packetEncoder) error {
	e.Push(&crcField{})
	e.PutInt8(m.MagicByte)
	e.PutInt8(m.Attributes)
	if err := e.PutBytes(m.Key); err != nil {
		return err
	}
	if err := e.PutBytes(m.Value); err != nil {
		return err
	}
	e.PutInt16(int16(len(m.Headers)))
	for key, header := range m.Headers {
		if err := e.PutString(key); err != nil {
			return err
		}
		if err := e.PutBytes(header); err != nil {
			return err
		}
	}
	e.Pop()
	return nil
}

// crcField is used to perform a CRC32 check on a message.
type crcField struct {
	StartOffset int
}

// SaveOffset sets the position to fill the CRC digest.
func (f *crcField) SaveOffset(in int) {
	f.StartOffset = in
}

// ReserveSize sets the number of bytes to reserve for the CRC digest.
func (f *crcField) ReserveSize() int {
	return 4
}

// Fill sets the CRC digest.
func (f *crcField) Fill(curOffset int, buf []byte) error {
	crc := crc32.Checksum(buf[f.StartOffset+4:curOffset], crc32cTable)
	encoding.PutUint32(buf[f.StartOffset:], crc)
	return nil
}

// Check the CRC digest.
func (f *crcField) Check(curOffset int, buf []byte) error {
	crc := crc32.Checksum(buf[f.StartOffset+4:curOffset], crc32cTable)
	if crc != encoding.Uint32(buf[f.StartOffset:]) {
		return errors.New("crc didn't match")
	}
	return nil
}

// SerializedMessage is a serialized message read from the log.
type SerializedMessage []byte

// Crc returns the CRC32 digest of the message.
func (m SerializedMessage) Crc() uint32 {
	return encoding.Uint32(m)
}

// MagicByte returns the byte used for encoding protocol version detection.
func (m SerializedMessage) MagicByte() int8 {
	return int8(m[4])
}

// Attributes returns the byte used for message flags.
func (m SerializedMessage) Attributes() int8 {
	return int8(m[5])
}

// Key returns the message key.
func (m SerializedMessage) Key() []byte {
	start, end, size := m.keyOffsets()
	if size == -1 {
		return nil
	}
	return m[start+4 : end]
}

// Value returns the message value.
func (m SerializedMessage) Value() []byte {
	start, end, size := m.valueOffsets()
	if size == -1 {
		return nil
	}
	return m[start+4 : end]
}

// Headers returns the message headers map.
func (m SerializedMessage) Headers() map[string][]byte {
	var (
		_, valueEnd, _ = m.valueOffsets()
		n              = valueEnd
		numHeaders     = encoding.Uint16(m[n:])
		headers        = make(map[string][]byte, numHeaders)
	)
	n += 2
	for i := uint16(0); i < numHeaders; i++ {
		keySize := encoding.Uint16(m[n:])
		n += 2
		key := string(m[n : n+int32(keySize)])
		n += int32(keySize)
		valueSize := encoding.Uint32(m[n:])
		n += 4
		value := m[n : n+int32(valueSize)]
		n += int32(valueSize)
		headers[key] = value
	}
	return headers
}

func (m SerializedMessage) keyOffsets() (start, end, size int32) {
	start = 6
	size = int32(encoding.Uint32(m[start:]))
	end = start + 4
	if size != -1 {
		end += size
	}
	return
}

func (m SerializedMessage) valueOffsets() (start, end, size int32) {
	_, keyEnd, _ := m.keyOffsets()
	start = keyEnd
	size = int32(encoding.Uint32(m[start:]))
	end = start + 4
	if size != -1 {
		end += size
	}
	return
}
