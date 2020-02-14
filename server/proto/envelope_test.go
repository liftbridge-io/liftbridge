package proto

import (
	"testing"
	"time"

	client "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/stretchr/testify/require"
)

// Ensure we can marshal a message and then unmarshal it.
func TestMarshalUnmarshalEnvelopeMessage(t *testing.T) {
	msg := &client.Message{
		Offset:       42,
		Key:          []byte("foo"),
		Value:        []byte("hello"),
		Timestamp:    time.Now().UnixNano(),
		Stream:       "foo",
		Subject:      "foo",
		ReplySubject: "reply",
		Headers: map[string][]byte{
			"foo": []byte("bar"),
			"baz": []byte("qux"),
		},
		AckInbox:      "ack",
		CorrelationId: "123",
	}

	envelope, err := MarshalEnvelope(msg)
	require.NoError(t, err)

	unmarshaled := new(client.Message)
	require.NoError(t, UnmarshalEnvelope(envelope, unmarshaled))

	require.Equal(t, msg, unmarshaled)
}

// Ensure we can marshal an ack and then unmarshal it.
func TestMarshalUnmarshalEnvelopeAck(t *testing.T) {
	ack := &client.Ack{
		Offset:           42,
		Stream:           "foo",
		MsgSubject:       "foo",
		AckInbox:         "ack",
		CorrelationId:    "123",
		PartitionSubject: "foo.1",
	}

	envelope, err := MarshalEnvelope(ack)
	require.NoError(t, err)

	unmarshaled := new(client.Ack)
	require.NoError(t, UnmarshalEnvelope(envelope, unmarshaled))

	require.Equal(t, ack, unmarshaled)
}

// Ensure UnmarshalEnvelope returns an error if there is not enough data for an
// envelope.
func TestUnmarshalEnvelopeUnderflow(t *testing.T) {
	require.Error(t, UnmarshalEnvelope([]byte{}, new(client.Ack)))
}

// Ensure UnmarshalEnvelope returns an error if the magic number is different.
func TestUnmarshalEnvelopeUnexpectedMagicNumber(t *testing.T) {
	require.Error(t, UnmarshalEnvelope([]byte("foobarbaz"), new(client.Ack)))
}

// Ensure UnmarshalEnvelope returns an error if the protocol version is
// unknown.
func TestUnmarshalEnvelopeUnexpectedProtoVersion(t *testing.T) {
	msg, err := MarshalEnvelope(new(client.Message))
	require.NoError(t, err)
	msg[4] = 0x01
	require.Error(t, UnmarshalEnvelope(msg, new(client.Message)))
}

// Ensure UnmarshalEnvelope returns an error if the CRC flag is set but no CRC
// is present.
func TestUnmarshalEnvelopeMissingCRC(t *testing.T) {
	msg, err := MarshalEnvelope(new(client.Message))
	require.NoError(t, err)
	msg[6] = setBit(msg[6], 0)
	require.Error(t, UnmarshalEnvelope(msg, new(client.Message)))
}

// Ensure UnmarshalEnvelope returns an error if the CRC flag is set but the CRC
// doesn't match the expected CRC.
func TestUnmarshalEnvelopeMismatchedCRC(t *testing.T) {
	msg, err := MarshalEnvelope(new(client.Message))
	require.NoError(t, err)
	msg[6] = setBit(msg[6], 0)
	buf := make([]byte, len(msg)+4)
	copy(buf, msg[:8])
	buf[8] = byte(32)
	copy(buf[12:], msg[8:])
	buf[5] = byte(12)
	require.Error(t, UnmarshalEnvelope(buf, new(client.Message)))
}

func setBit(n byte, pos uint8) byte {
	n |= (1 << pos)
	return n
}
