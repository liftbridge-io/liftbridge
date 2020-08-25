package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	client "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/stretchr/testify/require"
)

// Ensure we can marshal a message and then unmarshal it.
func TestMarshalUnmarshalPublish(t *testing.T) {
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

	envelope, err := MarshalPublish(msg)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalPublish(envelope)
	require.NoError(t, err)

	require.Equal(t, msg, unmarshaled)
}

// Ensure we can marshal an ack and then unmarshal it.
func TestMarshalUnmarshalAck(t *testing.T) {
	ack := &client.Ack{
		Offset:           42,
		Stream:           "foo",
		MsgSubject:       "foo",
		AckInbox:         "ack",
		CorrelationId:    "123",
		PartitionSubject: "foo.1",
		Timestamp:        43,
	}

	envelope, err := MarshalAck(ack)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalAck(envelope)
	require.NoError(t, err)

	require.Equal(t, ack, unmarshaled)
}

// Ensure we can marshal a ServerInfoRequest and then unmarshal it.
func TestMarshalUnmarshalServerInfoRequest(t *testing.T) {
	req := &ServerInfoRequest{
		Id: "foo",
	}
	envelope, err := MarshalServerInfoRequest(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalServerInfoRequest(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a ServerInfoResponse and then unmarshal it.
func TestMarshalUnmarshalServerInfoResponse(t *testing.T) {
	req := &ServerInfoResponse{
		Id:   "foo",
		Host: "0.0.0.0",
		Port: 4000,
	}
	envelope, err := MarshalServerInfoResponse(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalServerInfoResponse(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a PropagatedRequest and then unmarshal it.
func TestMarshalUnmarshalPropagatedRequest(t *testing.T) {
	req := &PropagatedRequest{
		Op: Op_CREATE_STREAM,
		CreateStreamOp: &CreateStreamOp{
			Stream: &Stream{
				Name:    "foo",
				Subject: "foo",
				Partitions: []*Partition{{
					Subject:           "foo",
					Stream:            "foo",
					ReplicationFactor: 3,
				}},
			},
		},
	}
	envelope, err := MarshalPropagatedRequest(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalPropagatedRequest(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a PropagatedResponse and then unmarshal it.
func TestMarshalUnmarshalPropagatedResponse(t *testing.T) {
	req := &PropagatedResponse{Op: Op_CREATE_STREAM}
	envelope, err := MarshalPropagatedResponse(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalPropagatedResponse(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a PartitionStatusRequest and then unmarshal it.
func TestMarshalUnmarshalPartitionStatusRequest(t *testing.T) {
	req := &PartitionStatusRequest{
		Stream:    "foo",
		Partition: 1,
	}
	envelope, err := MarshalPartitionStatusRequest(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalPartitionStatusRequest(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a PartitionStatusResponse and then unmarshal it.
func TestMarshalUnmarshalPartitionStatusResponse(t *testing.T) {
	req := &PartitionStatusResponse{
		Exists: false,
	}
	envelope, err := MarshalPartitionStatusResponse(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalPartitionStatusResponse(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a ReplicationRequest and then unmarshal it.
func TestMarshalUnmarshalReplicationRequest(t *testing.T) {
	req := &ReplicationRequest{
		ReplicaID: "b",
		Offset:    10,
	}
	envelope, err := MarshalReplicationRequest(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalReplicationRequest(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a ReplicationResponse and then unmarshal it.
func TestMarshalUnmarshalReplicationResponse(t *testing.T) {
	buf := new(bytes.Buffer)
	n := WriteReplicationResponseHeader(buf)
	require.Equal(t, 8, n)

	var (
		epoch = uint64(2)
		hw    = int64(100)
		data  = []byte("blah")
	)

	// Write the leader epoch.
	binary.Write(buf, Encoding, epoch)
	// Write the HW.
	binary.Write(buf, Encoding, hw)
	// Write some fake message data.
	buf.Write(data)

	unmarshaledEpoch, unmarshaledHW, unmarshaledData, err := UnmarshalReplicationResponse(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, epoch, unmarshaledEpoch)
	require.Equal(t, hw, unmarshaledHW)
	require.Equal(t, data, unmarshaledData)
}

// Ensure we can marshal a LeaderEpochOffsetRequest and then unmarshal it.
func TestMarshalUnmarshalLeaderEpochOffsetRequest(t *testing.T) {
	req := &LeaderEpochOffsetRequest{
		LeaderEpoch: 1,
	}
	envelope, err := MarshalLeaderEpochOffsetRequest(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalLeaderEpochOffsetRequest(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a LeaderEpochOffsetResponse and then unmarshal it.
func TestMarshalUnmarshalLeaderEpochOffsetResponse(t *testing.T) {
	req := &LeaderEpochOffsetResponse{
		EndOffset: 10,
	}
	envelope, err := MarshalLeaderEpochOffsetResponse(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalLeaderEpochOffsetResponse(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a PartitionNotification and then unmarshal it.
func TestMarshalUnmarshalPartitionNotification(t *testing.T) {
	req := &PartitionNotification{
		Stream:    "foo",
		Partition: 2,
	}
	envelope, err := MarshalPartitionNotification(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalPartitionNotification(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a RaftJoinRequest and then unmarshal it.
func TestMarshalUnmarshalRaftJoinRequest(t *testing.T) {
	req := &RaftJoinRequest{
		NodeID:   "foo",
		NodeAddr: "bar",
	}
	envelope, err := MarshalRaftJoinRequest(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalRaftJoinRequest(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure we can marshal a RaftJoinResponse and then unmarshal it.
func TestMarshalUnmarshalRaftJoinResponse(t *testing.T) {
	req := &RaftJoinResponse{}
	envelope, err := MarshalRaftJoinResponse(req)
	require.NoError(t, err)

	unmarshaled, err := UnmarshalRaftJoinResponse(envelope)
	require.NoError(t, err)

	require.Equal(t, req, unmarshaled)
}

// Ensure unmarshalEnvelope returns an error if there is not enough data for an
// envelope.
func TestUnmarshalEnvelopeUnderflow(t *testing.T) {
	_, err := UnmarshalAck([]byte{})
	require.Error(t, err)
}

// Ensure unmarshalEnvelope returns an error if the magic number is different.
func TestUnmarshalEnvelopeUnexpectedMagicNumber(t *testing.T) {
	_, err := UnmarshalAck([]byte("foobarbaz"))
	require.Error(t, err)
}

// Ensure unmarshalEnvelope returns an error if the protocol version is
// unknown.
func TestUnmarshalEnvelopeUnexpectedProtoVersion(t *testing.T) {
	msg, err := MarshalPublish(new(client.Message))
	require.NoError(t, err)
	msg[4] = 0x01
	_, err = UnmarshalPublish(msg)
	require.Error(t, err)
}

// Ensure unmarshalEnvelope returns an error if the CRC flag is set but no CRC
// is present.
func TestUnmarshalEnvelopeMissingCRC(t *testing.T) {
	msg, err := MarshalPublish(new(client.Message))
	require.NoError(t, err)
	msg[6] = setBit(msg[6], 0)
	_, err = UnmarshalPublish(msg)
	require.Error(t, err)
}

// Ensure unmarshalEnvelope returns an error if the CRC flag is set but the CRC
// doesn't match the expected CRC.
func TestUnmarshalEnvelopeMismatchedCRC(t *testing.T) {
	msg, err := MarshalPublish(new(client.Message))
	require.NoError(t, err)
	msg[6] = setBit(msg[6], 0)
	buf := make([]byte, len(msg)+4)
	copy(buf, msg[:8])
	buf[8] = byte(32)
	copy(buf[12:], msg[8:])
	buf[5] = byte(12)
	_, err = UnmarshalPublish(buf)
	require.Error(t, err)
}

// Ensure unmarshalEnvelope returns an error if the envelope's MsgType doesn't
// match the expected type.
func TestUnmarshalEnvelopeMismatchedType(t *testing.T) {
	msg, err := MarshalPublish(new(client.Message))
	require.NoError(t, err)
	_, err = UnmarshalAck(msg)
	require.Error(t, err)
}

func setBit(n byte, pos uint8) byte {
	n |= (1 << pos)
	return n
}
