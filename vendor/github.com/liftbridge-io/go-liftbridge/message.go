package liftbridge

import (
	"bytes"

	"github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
)

// MessageOptions are used to configure optional settings for a Message.
type MessageOptions struct {
	// Key to set on the Message. If Liftbridge has stream compaction enabled,
	// the stream will retain only the last value for each key.
	Key []byte

	// AckInbox sets the NATS subject Liftbridge should publish the Message ack
	// to. If it's not set, Liftbridge will not send an ack.
	AckInbox string

	// CorrelationID sets the identifier used to correlate an ack with the
	// published Message. If it's not set, the ack will not have a correlation
	// id.
	CorrelationID string

	// AckPolicy controls the behavior of Message acks sent by the server. By
	// default, Liftbridge will send an ack when the stream leader has written
	// the message to its write-ahead log.
	AckPolicy proto.AckPolicy
}

// NewMessage returns a serialized message for the given payload and
// MessageOptions.
func NewMessage(value []byte, options MessageOptions) []byte {
	msg := &proto.Message{
		Key:           options.Key,
		Value:         value,
		AckInbox:      options.AckInbox,
		CorrelationId: options.CorrelationID,
		AckPolicy:     options.AckPolicy,
	}
	m, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	buf := make([]byte, envelopeCookieLen+len(m))
	copy(buf[0:], envelopeCookie)
	copy(buf[envelopeCookieLen:], m)
	return buf
}

// UnmarshalAck deserializes an Ack from the given byte slice. It returns an
// error if the given data is not actually an Ack.
func UnmarshalAck(data []byte) (*proto.Ack, error) {
	var (
		ack = &proto.Ack{}
		err = ack.Unmarshal(data)
	)
	return ack, err
}

// UnmarshalMessage deserializes a message from the given byte slice.  It
// returns a bool indicating if the given data was actually a Message or not.
func UnmarshalMessage(data []byte) (*proto.Message, bool) {
	if len(data) <= envelopeCookieLen {
		return nil, false
	}
	if !bytes.Equal(data[:envelopeCookieLen], envelopeCookie) {
		return nil, false
	}
	var (
		msg = &proto.Message{}
		err = msg.Unmarshal(data[envelopeCookieLen:])
	)
	if err != nil {
		return nil, false
	}
	return msg, true
}
