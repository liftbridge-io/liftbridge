package encryption

// Codec provides the necessary method to safely retrieve
// secret encryption key to encrypt/decrypt data on server side.
type Codec interface {

	// Seal takes the message, performs the encryption and returns
	// the bytes to store according to the following layout:
	// The first byte contains the size of the wrapped key, followed by the wrapped key and
	// finally the encrypted message.
	//
	// |  byte 0  |   byte 1   |   byte 2   |    ...   | byte (n +1) |    byte (n+2)  |  ... | byte (n + m + 2) |
	// |----------|------------|------------|----------|--------------|----------------|------|------------------|
	// | key size | key byte 0 | key byte 1 |      ... | key byte n   | message byte 0 |  ... |  message byte m  |

	Seal([]byte) ([]byte, error)

	// Read takes the cipher text, decrypts it and returns the decrypted data.
	// The incoming byte array has the following structure:
	// The first byte indicates the size of the wrapped key.
	// The n next bytes contain the wrapped key itself (n is the value of the first byte).
	// The remaining bytes are the message itself.
	//
	// |  byte 0  |   byte 1   |   byte 2   |    ...   | byte (n +1) |    byte (n+2)  |  ... | byte (n + m + 2) |
	// |----------|------------|------------|----------|--------------|----------------|------|------------------|
	// | key size | key byte 0 | key byte 1 |      ... | key byte n   | message byte 0 |  ... |  message byte m  |

	Read([]byte) ([]byte, error)
}
