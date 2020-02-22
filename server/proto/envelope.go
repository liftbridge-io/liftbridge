package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	pb "github.com/golang/protobuf/proto"
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

// MarshalEnvelope serializes a protobuf message into the Liftbridge envelope
// wire format.
func MarshalEnvelope(msg pb.Message) ([]byte, error) {
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
	buf[pos] = 0x00 // Reserved
	pos++
	if pos != headerLen {
		panic(fmt.Sprintf("Payload position (%d) does not match expected HeaderLen (%d)",
			pos, headerLen))
	}
	copy(buf[pos:], data)
	return buf, nil
}

// UnmarshalEnvelope deserializes a Liftbridge envelope into a protobuf
// message.
func UnmarshalEnvelope(data []byte, msg pb.Message) error {
	if len(data) < envelopeMinHeaderLen {
		return errors.New("data missing envelope header")
	}
	if !bytes.Equal(data[:envelopeMagicNumberLen], envelopeMagicNumber) {
		return errors.New("unexpected envelope magic number")
	}
	if data[4] != envelopeProtoV0 {
		return fmt.Errorf("unknown envelope protocol: %v", data[4])
	}

	var (
		headerLen = int(data[5])
		flags     = data[6]
		payload   = data[headerLen:]
	)

	// Check CRC.
	if hasBit(flags, 0) {
		// Make sure there is a CRC present.
		if headerLen != envelopeMinHeaderLen+4 {
			return errors.New("incorrect envelope header size")
		}
		crc := Encoding.Uint32(data[envelopeMinHeaderLen:headerLen])
		if c := crc32.Checksum(payload, crc32cTable); c != crc {
			return fmt.Errorf("crc mismatch: expected %d, got %d", crc, c)
		}
	}

	return pb.Unmarshal(payload, msg)
}

// hasBit checks if the given bit position is set on the provided byte.
func hasBit(n byte, pos uint8) bool {
	val := n & (1 << pos)
	return (val > 0)
}
