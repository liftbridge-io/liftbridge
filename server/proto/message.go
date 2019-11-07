package proto

import (
	client "github.com/liftbridge-io/liftbridge-api/go"
)

// Message is the serialized object that gets written to the log.
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
}

// Encode the Message into the PacketEncoder.
func (m *Message) Encode(e PacketEncoder) error {
	e.Push(&CRCField{})
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
