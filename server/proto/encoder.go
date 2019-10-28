package proto

import (
	"encoding/binary"
	"errors"
	"math"
)

var (
	// Encoding is the byte order to use for serialization.
	Encoding = binary.BigEndian

	errInvalidStringLength    = errors.New("invalid string length")
	errInvalidArrayLength     = errors.New("invalid array length")
	errInvalidByteSliceLength = errors.New("invalid byteslice length")
)

// PacketEncoder is used to serialize an object.
type PacketEncoder interface {
	PutBool(in bool)
	PutInt8(in int8)
	PutInt16(in int16)
	PutInt32(in int32)
	PutInt64(in int64)
	PutArrayLength(in int) error
	PutRawBytes(in []byte) error
	PutBytes(in []byte) error
	PutString(in string) error
	PutNullableString(in *string) error
	PutStringArray(in []string) error
	PutInt32Array(in []int32) error
	PutInt64Array(in []int64) error
	Push(pe PushEncoder)
	Pop()
}

// PushEncoder is used to push an operation onto the stack to perform later
// once serialized bytes are filled.
type PushEncoder interface {
	SaveOffset(in int)
	ReserveSize() int
	Fill(curOffset int, buf []byte) error
}

// Encoder is a struct that can be serialized.
type Encoder interface {
	Encode(e PacketEncoder) error
}

// Encode serializes the struct to bytes.
func Encode(e Encoder) ([]byte, error) {
	lenEnc := new(LenEncoder)
	err := e.Encode(lenEnc)
	if err != nil {
		return nil, err
	}

	b := make([]byte, lenEnc.Length)
	byteEnc := NewByteEncoder(b)
	err = e.Encode(byteEnc)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// LenEncoder is a PacketEncoder that tracks the running length of serialized
// bytes.
type LenEncoder struct {
	Length int
}

// PutBool increments length for a bool.
func (e *LenEncoder) PutBool(in bool) {
	e.Length++
}

// PutInt8 increments length for an int8.
func (e *LenEncoder) PutInt8(in int8) {
	e.Length++
}

// PutInt16 increments length for an int16.
func (e *LenEncoder) PutInt16(in int16) {
	e.Length += 2
}

// PutInt32 increments length for an int32.
func (e *LenEncoder) PutInt32(in int32) {
	e.Length += 4
}

// PutInt64 increments length for an int64.
func (e *LenEncoder) PutInt64(in int64) {
	e.Length += 8
}

// PutArrayLength increments length for an array size.
func (e *LenEncoder) PutArrayLength(in int) error {
	if in > math.MaxInt32 {
		return errInvalidArrayLength
	}
	e.Length += 4
	return nil
}

// arrays

// PutBytes increments length for a size-prefixed byte array.
func (e *LenEncoder) PutBytes(in []byte) error {
	e.Length += 4
	if in == nil {
		return nil
	}
	if len(in) > math.MaxInt32 {
		return errInvalidByteSliceLength
	}
	e.Length += len(in)
	return nil
}

// PutRawBytes increments length for a raw byte array.
func (e *LenEncoder) PutRawBytes(in []byte) error {
	if len(in) > math.MaxInt32 {
		return errInvalidByteSliceLength
	}
	e.Length += len(in)
	return nil
}

// PutString increments length for a string.
func (e *LenEncoder) PutString(in string) error {
	e.Length += 2
	if len(in) > math.MaxInt16 {
		return errInvalidStringLength
	}
	e.Length += len(in)
	return nil
}

// PutNullableString increments length for a nullable string.
func (e *LenEncoder) PutNullableString(in *string) error {
	if in == nil {
		e.Length += 2
		return nil
	}
	return e.PutString(*in)
}

// PutStringArray increments length for a string array.
func (e *LenEncoder) PutStringArray(in []string) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, str := range in {
		if err := e.PutString(str); err != nil {
			return err
		}
	}

	return nil
}

// PutInt32Array increments length for an int32 array.
func (e *LenEncoder) PutInt32Array(in []int32) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	e.Length += 4 * len(in)
	return nil
}

// PutInt64Array increments length for an int64 array.
func (e *LenEncoder) PutInt64Array(in []int64) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	e.Length += 8 * len(in)
	return nil
}

// Push increments length based on the PushEncoder's reserved size.
func (e *LenEncoder) Push(pe PushEncoder) {
	e.Length += pe.ReserveSize()
}

// Pop is a no-op.
func (e *LenEncoder) Pop() {}

// ByteEncoder is a PacketEncoder that serializes data into a byte slice.
type ByteEncoder struct {
	b     []byte
	off   int
	stack []PushEncoder
}

// Bytes returns the underlying byte slice.
func (e *ByteEncoder) Bytes() []byte {
	return e.b
}

// NewByteEncoder creates a new ByteEncoder with the given backing
// pre-allocated byte slice.
func NewByteEncoder(b []byte) *ByteEncoder {
	return &ByteEncoder{b: b}
}

// PutBool serializes a bool.
func (e *ByteEncoder) PutBool(in bool) {
	if in {
		e.b[e.off] = byte(int8(1))
	}
	e.off++
}

// PutInt8 serializes an int8.
func (e *ByteEncoder) PutInt8(in int8) {
	e.b[e.off] = byte(in)
	e.off++
}

// PutInt16 serializes an int16.
func (e *ByteEncoder) PutInt16(in int16) {
	Encoding.PutUint16(e.b[e.off:], uint16(in))
	e.off += 2
}

// PutInt32 serializes an int32.
func (e *ByteEncoder) PutInt32(in int32) {
	Encoding.PutUint32(e.b[e.off:], uint32(in))
	e.off += 4
}

// PutInt64 serializes an int64.
func (e *ByteEncoder) PutInt64(in int64) {
	Encoding.PutUint64(e.b[e.off:], uint64(in))
	e.off += 8
}

// PutArrayLength serializes an array length as an int32.
func (e *ByteEncoder) PutArrayLength(in int) error {
	e.PutInt32(int32(in))
	return nil
}

// PutRawBytes serializes a byte slice.
func (e *ByteEncoder) PutRawBytes(in []byte) error {
	copy(e.b[e.off:], in)
	e.off += len(in)
	return nil
}

// PutBytes serializes a size-prefixed byte slice.
func (e *ByteEncoder) PutBytes(in []byte) error {
	if in == nil {
		e.PutInt32(-1)
		return nil
	}
	e.PutInt32(int32(len(in)))
	copy(e.b[e.off:], in)
	e.off += len(in)
	return nil
}

// PutString serializes a size-prefixed string.
func (e *ByteEncoder) PutString(in string) error {
	e.PutInt16(int16(len(in)))
	copy(e.b[e.off:], in)
	e.off += len(in)
	return nil
}

// PutNullableString serializes a nullable string.
func (e *ByteEncoder) PutNullableString(in *string) error {
	if in == nil {
		e.PutInt16(-1)
		return nil
	}
	return e.PutString(*in)
}

// PutStringArray serializes a string array.
func (e *ByteEncoder) PutStringArray(in []string) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, val := range in {
		if err := e.PutString(val); err != nil {
			return err
		}
	}

	return nil
}

// PutInt32Array serializes an int32 array.
func (e *ByteEncoder) PutInt32Array(in []int32) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		e.PutInt32(val)
	}
	return nil
}

// PutInt64Array serializes an int64 array.
func (e *ByteEncoder) PutInt64Array(in []int64) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		e.PutInt64(val)
	}
	return nil
}

// Push adds the given PushEncoder to the stack and saves the current offset
// position.
func (e *ByteEncoder) Push(pe PushEncoder) {
	pe.SaveOffset(e.off)
	e.off += pe.ReserveSize()
	e.stack = append(e.stack, pe)
}

// Pop the stack and run the popped PushEncoder on the serialized data.
func (e *ByteEncoder) Pop() {
	// this is go's ugly pop pattern (the inverse of append)
	pe := e.stack[len(e.stack)-1]
	e.stack = e.stack[:len(e.stack)-1]
	pe.Fill(e.off, e.b)
}
