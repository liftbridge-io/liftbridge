package proto

import (
	"errors"
	"hash/crc32"
)

// CRCField is used to perform a CRC32 check on a message.
type CRCField struct {
	StartOffset int
}

// SaveOffset sets the position to fill the CRC digest.
func (f *CRCField) SaveOffset(in int) {
	f.StartOffset = in
}

// ReserveSize sets the number of bytes to reserve for the CRC digest.
func (f *CRCField) ReserveSize() int {
	return 4
}

// Fill sets the CRC digest.
func (f *CRCField) Fill(curOffset int, buf []byte) error {
	crc := crc32.ChecksumIEEE(buf[f.StartOffset+4 : curOffset])
	Encoding.PutUint32(buf[f.StartOffset:], crc)
	return nil
}

// Check the CRC digest.
func (f *CRCField) Check(curOffset int, buf []byte) error {
	crc := crc32.ChecksumIEEE(buf[f.StartOffset+4 : curOffset])
	if crc != Encoding.Uint32(buf[f.StartOffset:]) {
		return errors.New("crc didn't match")
	}
	return nil
}
