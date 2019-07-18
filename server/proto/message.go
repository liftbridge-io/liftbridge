package proto

import client "github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"

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

func (m *Message) Decode(d PacketDecoder) error {
	var err error
	if err = d.Push(&CRCField{}); err != nil {
		return err
	}
	if m.MagicByte, err = d.Int8(); err != nil {
		return err
	}
	if m.Attributes, err = d.Int8(); err != nil {
		return err
	}
	if m.Key, err = d.Bytes(); err != nil {
		return err
	}
	if m.Value, err = d.Bytes(); err != nil {
		return err
	}
	numHeaders, err := d.Int16()
	if err != nil {
		return err
	}
	m.Headers = make(map[string][]byte, numHeaders)
	for i := int16(0); i < numHeaders; i++ {
		key, err := d.String()
		if err != nil {
			return err
		}
		value, err := d.Bytes()
		if err != nil {
			return err
		}
		m.Headers[key] = value
	}
	return d.Pop()
}
