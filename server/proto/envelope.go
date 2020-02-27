package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	pb "github.com/golang/protobuf/proto"
	client "github.com/liftbridge-io/liftbridge-api/go"
)

// msgType indicates the type of message contained by an envelope.
type msgType byte

const (
	msgTypePublish msgType = iota
	msgTypeAck

	msgTypeReplicationRequest
	msgTypeReplicationResponse

	msgTypeRaftJoinRequest
	msgTypeRaftJoinResponse

	msgTypeLeaderEpochOffsetRequest
	msgTypeLeaderEpochOffsetResponse

	msgTypePropagatedRequest
	msgTypePropagatedResponse

	msgTypeServerInfoRequest
	msgTypeServerInfoResponse

	msgTypePartitionStatusRequest
	msgTypePartitionStatusResponse

	msgTypePartitionNotification
)

const (
	// envelopeProtoV0 is version 0 of the envelope protocol.
	envelopeProtoV0 = 0x00

	// envelopeMinHeaderLen is the minimum length of the envelope header, i.e.
	// without CRC-32C set.
	envelopeMinHeaderLen = 8
)

var (
	// Encoding is the byte order to use for protocol serialization.
	Encoding = binary.BigEndian

	// envelopeMagicNumber is a value that indicates if a NATS message is a
	// structured message protobuf. This was chosen by random but deliberately
	// restricted to invalid UTF-8 to reduce the chance of a collision. This
	// was also verified to not match known file signatures.
	envelopeMagicNumber    = []byte{0xB9, 0x0E, 0x43, 0xB4}
	envelopeMagicNumberLen = len(envelopeMagicNumber)

	crc32cTable = crc32.MakeTable(crc32.Castagnoli)
)

// MarshalPublish serializes a protobuf publish message into the Liftbridge
// envelope wire format.
func MarshalPublish(msg *client.Message) ([]byte, error) {
	return marshalEnvelope(msg, msgTypePublish)
}

// MarshalAck serializes a protobuf ack message into the Liftbridge envelope
// wire format.
func MarshalAck(ack *client.Ack) ([]byte, error) {
	return marshalEnvelope(ack, msgTypeAck)
}

// MarshalServerInfoRequest serializes a ServerInfoRequest protobuf into the
// Liftbridge envelope wire format.
func MarshalServerInfoRequest(req *ServerInfoRequest) ([]byte, error) {
	return marshalEnvelope(req, msgTypeServerInfoRequest)
}

// MarshalServerInfoResponse serializes a ServerInfoResponse protobuf into the
// Liftbridge envelope wire format.
func MarshalServerInfoResponse(req *ServerInfoResponse) ([]byte, error) {
	return marshalEnvelope(req, msgTypeServerInfoResponse)
}

// MarshalPropagatedRequest serializes a PropagatedRequest protobuf into the
// Liftbridge envelope wire format.
func MarshalPropagatedRequest(req *PropagatedRequest) ([]byte, error) {
	return marshalEnvelope(req, msgTypePropagatedRequest)
}

// MarshalPropagatedResponse serializes a PropagatedResponse protobuf into the
// Liftbridge envelope wire format.
func MarshalPropagatedResponse(req *PropagatedResponse) ([]byte, error) {
	return marshalEnvelope(req, msgTypePropagatedResponse)
}

// MarshalPartitionStatusRequest serializes a PartitionStatusRequest protobuf
// into the Liftbridge envelope wire format.
func MarshalPartitionStatusRequest(req *PartitionStatusRequest) ([]byte, error) {
	return marshalEnvelope(req, msgTypePartitionStatusRequest)
}

// MarshalPartitionStatusResponse serializes a PartitionStatusResponse protobuf
// into the Liftbridge envelope wire format.
func MarshalPartitionStatusResponse(resp *PartitionStatusResponse) ([]byte, error) {
	return marshalEnvelope(resp, msgTypePartitionStatusResponse)
}

// MarshalReplicationRequest serializes a ReplicationRequest protobuf into the
// Liftbridge envelope wire format.
func MarshalReplicationRequest(req *ReplicationRequest) ([]byte, error) {
	return marshalEnvelope(req, msgTypeReplicationRequest)
}

// MarshalLeaderEpochOffsetRequest serializes a LeaderEpochOffsetRequest
// protobuf into the Liftbridge envelope wire format.
func MarshalLeaderEpochOffsetRequest(req *LeaderEpochOffsetRequest) ([]byte, error) {
	return marshalEnvelope(req, msgTypeLeaderEpochOffsetRequest)
}

// MarshalLeaderEpochOffsetResponse serializes a LeaderEpochOffsetResponse
// protobuf into the Liftbridge envelope wire format.
func MarshalLeaderEpochOffsetResponse(req *LeaderEpochOffsetResponse) ([]byte, error) {
	return marshalEnvelope(req, msgTypeLeaderEpochOffsetResponse)
}

// MarshalPartitionNotification serializes a PartitionNotification protobuf
// into the Liftbridge envelope wire format.
func MarshalPartitionNotification(req *PartitionNotification) ([]byte, error) {
	return marshalEnvelope(req, msgTypePartitionNotification)
}

// MarshalRaftJoinRequest serializes a RaftJoinRequest protobuf into the
// Liftbridge envelope wire format.
func MarshalRaftJoinRequest(req *RaftJoinRequest) ([]byte, error) {
	return marshalEnvelope(req, msgTypeRaftJoinRequest)
}

// MarshalRaftJoinResponse serializes a RaftJoinResponse protobuf into the
// Liftbridge envelope wire format.
func MarshalRaftJoinResponse(req *RaftJoinResponse) ([]byte, error) {
	return marshalEnvelope(req, msgTypeRaftJoinResponse)
}

// WriteReplicationResponseHeader writes the envelope protocol header for
// replication messages to the buffer and returns the number of bytes written.
func WriteReplicationResponseHeader(buf *bytes.Buffer) int {
	buf.Write(envelopeMagicNumber)
	buf.WriteByte(envelopeProtoV0)
	buf.WriteByte(byte(envelopeMinHeaderLen))
	buf.WriteByte(0x00)
	buf.WriteByte(byte(msgTypeReplicationResponse))
	return 8
}

// marshalEnvelope serializes a protobuf message into the Liftbridge envelope
// wire format.
func marshalEnvelope(msg pb.Message, msgType msgType) ([]byte, error) {
	data, err := pb.Marshal(msg)
	if err != nil {
		return nil, err
	}

	var (
		buf       = make([]byte, envelopeMagicNumberLen+4+len(data))
		pos       = 0
		headerLen = envelopeMinHeaderLen
	)
	copy(buf[pos:], envelopeMagicNumber)
	pos += envelopeMagicNumberLen
	buf[pos] = envelopeProtoV0 // Version
	pos++
	buf[pos] = byte(headerLen) // HeaderLen
	pos++
	buf[pos] = 0x00 // Flags
	pos++
	buf[pos] = byte(msgType) // MsgType
	pos++
	if pos != headerLen {
		panic(fmt.Sprintf("Payload position (%d) does not match expected HeaderLen (%d)",
			pos, headerLen))
	}
	copy(buf[pos:], data)
	return buf, nil
}

// UnmarshalPublish deserializes a Liftbridge publish envelope into a protobuf
// message.
func UnmarshalPublish(data []byte) (*client.Message, error) {
	var (
		msg = new(client.Message)
		err = unmarshalEnvelope(data, msg, msgTypePublish)
	)
	return msg, err
}

// UnmarshalAck deserializes a Liftbridge ack envelope into a protobuf message.
func UnmarshalAck(data []byte) (*client.Ack, error) {
	var (
		ack = new(client.Ack)
		err = unmarshalEnvelope(data, ack, msgTypeAck)
	)
	return ack, err
}

// UnmarshalPropagatedRequest deserializes a Liftbridge PropagatedRequest
// envelope into a protobuf message.
func UnmarshalPropagatedRequest(data []byte) (*PropagatedRequest, error) {
	var (
		req = new(PropagatedRequest)
		err = unmarshalEnvelope(data, req, msgTypePropagatedRequest)
	)
	return req, err
}

// UnmarshalPropagatedResponse deserializes a Liftbridge PropagatedResponse
// envelope into a protobuf message.
func UnmarshalPropagatedResponse(data []byte) (*PropagatedResponse, error) {
	var (
		resp = new(PropagatedResponse)
		err  = unmarshalEnvelope(data, resp, msgTypePropagatedResponse)
	)
	return resp, err
}

// UnmarshalServerInfoRequest deserializes a Liftbridge ServerInfoRequest
// envelope into a protobuf message.
func UnmarshalServerInfoRequest(data []byte) (*ServerInfoRequest, error) {
	var (
		req = new(ServerInfoRequest)
		err = unmarshalEnvelope(data, req, msgTypeServerInfoRequest)
	)
	return req, err
}

// UnmarshalServerInfoResponse deserializes a Liftbridge ServerInfoResponse
// envelope into a protobuf message.
func UnmarshalServerInfoResponse(data []byte) (*ServerInfoResponse, error) {
	var (
		resp = new(ServerInfoResponse)
		err  = unmarshalEnvelope(data, resp, msgTypeServerInfoResponse)
	)
	return resp, err
}

// UnmarshalPartitionStatusRequest deserializes a Liftbridge
// PartitionStatusRequest envelope into a protobuf message.
func UnmarshalPartitionStatusRequest(data []byte) (*PartitionStatusRequest, error) {
	var (
		req = new(PartitionStatusRequest)
		err = unmarshalEnvelope(data, req, msgTypePartitionStatusRequest)
	)
	return req, err
}

// UnmarshalPartitionStatusResponse deserializes a Liftbridge
// PartitionStatusResponse envelope into a protobuf message.
func UnmarshalPartitionStatusResponse(data []byte) (*PartitionStatusResponse, error) {
	var (
		resp = new(PartitionStatusResponse)
		err  = unmarshalEnvelope(data, resp, msgTypePartitionStatusResponse)
	)
	return resp, err
}

// UnmarshalRaftJoinRequest deserializes a Liftbridge RaftJoinRequest envelope
// into a protobuf message.
func UnmarshalRaftJoinRequest(data []byte) (*RaftJoinRequest, error) {
	var (
		req = new(RaftJoinRequest)
		err = unmarshalEnvelope(data, req, msgTypeRaftJoinRequest)
	)
	return req, err
}

// UnmarshalRaftJoinResponse deserializes a Liftbridge RaftJoinResponse
// envelope into a protobuf message.
func UnmarshalRaftJoinResponse(data []byte) (*RaftJoinResponse, error) {
	var (
		resp = new(RaftJoinResponse)
		err  = unmarshalEnvelope(data, resp, msgTypeRaftJoinResponse)
	)
	return resp, err
}

// UnmarshalPartitionNotification deserializes a Liftbridge
// PartitionNotification envelope into a protobuf message.
func UnmarshalPartitionNotification(data []byte) (*PartitionNotification, error) {
	var (
		req = new(PartitionNotification)
		err = unmarshalEnvelope(data, req, msgTypePartitionNotification)
	)
	return req, err
}

// UnmarshalLeaderEpochOffsetRequest deserializes a Liftbridge
// LeaderEpochOffsetRequest envelope into a protobuf message.
func UnmarshalLeaderEpochOffsetRequest(data []byte) (*LeaderEpochOffsetRequest, error) {
	var (
		req = new(LeaderEpochOffsetRequest)
		err = unmarshalEnvelope(data, req, msgTypeLeaderEpochOffsetRequest)
	)
	return req, err
}

// UnmarshalLeaderEpochOffsetResponse deserializes a Liftbridge
// LeaderEpochOffsetResponse envelope into a protobuf message.
func UnmarshalLeaderEpochOffsetResponse(data []byte) (*LeaderEpochOffsetResponse, error) {
	var (
		resp = new(LeaderEpochOffsetResponse)
		err  = unmarshalEnvelope(data, resp, msgTypeLeaderEpochOffsetResponse)
	)
	return resp, err
}

// UnmarshalReplicationRequest deserializes a Liftbridge ReplicationRequest
// envelope into a protobuf message.
func UnmarshalReplicationRequest(data []byte) (*ReplicationRequest, error) {
	var (
		req = new(ReplicationRequest)
		err = unmarshalEnvelope(data, req, msgTypeReplicationRequest)
	)
	return req, err
}

// UnmarshalReplicationResponse deserializes a Liftbridge replication response
// envelope and returns the leader epoch, HW, and message data.
func UnmarshalReplicationResponse(data []byte) (uint64, int64, []byte, error) {
	payload, err := checkEnvelope(data, msgTypeReplicationResponse)
	if err != nil {
		return 0, 0, nil, err
	}

	// We should have at least 16 bytes, 8 for leader epoch and 8 for HW.
	if len(payload) < 16 {
		return 0, 0, nil, errors.New("not enough data")
	}

	var (
		leaderEpoch = Encoding.Uint64(payload[:8])
		hw          = int64(Encoding.Uint64(payload[8:]))
	)

	return leaderEpoch, hw, payload[16:], nil
}

// unmarshalEnvelope deserializes a Liftbridge envelope into a protobuf
// message.
func unmarshalEnvelope(data []byte, msg pb.Message, msgType msgType) error {
	payload, err := checkEnvelope(data, msgType)
	if err != nil {
		return err
	}
	return pb.Unmarshal(payload, msg)
}

func checkEnvelope(data []byte, expectedType msgType) ([]byte, error) {
	if len(data) < envelopeMinHeaderLen {
		return nil, errors.New("data missing envelope header")
	}
	if !bytes.Equal(data[:envelopeMagicNumberLen], envelopeMagicNumber) {
		return nil, errors.New("unexpected envelope magic number")
	}
	if data[4] != envelopeProtoV0 {
		return nil, fmt.Errorf("unknown envelope protocol: %v", data[4])
	}

	var (
		headerLen  = int(data[5])
		flags      = data[6]
		actualType = msgType(data[7])
		payload    = data[headerLen:]
	)

	if actualType != expectedType {
		return nil, fmt.Errorf("MsgType mismatch: expected %v, got %v", expectedType, actualType)
	}

	// Check CRC.
	if hasBit(flags, 0) {
		// Make sure there is a CRC present.
		if headerLen != envelopeMinHeaderLen+4 {
			return nil, errors.New("incorrect envelope header size")
		}
		crc := Encoding.Uint32(data[envelopeMinHeaderLen:headerLen])
		if c := crc32.Checksum(payload, crc32cTable); c != crc {
			return nil, fmt.Errorf("crc mismatch: expected %d, got %d", crc, c)
		}
	}

	return payload, nil
}

// hasBit checks if the given bit position is set on the provided byte.
func hasBit(n byte, pos uint8) bool {
	val := n & (1 << pos)
	return (val > 0)
}
