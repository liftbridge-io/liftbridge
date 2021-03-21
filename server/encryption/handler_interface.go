package encryption

// Handler provides the necessary method to safely retrieve
// secret encryption key to encrypt/decrypt data at rest
type Handler interface {
	// Generate Data Key
	generateDKS() ([]byte, error)
	// Wrap Data Key
	wrapDKS([]byte) ([]byte, error)
	// Encrypt data using data key
	encryptData([]byte, []byte) ([]byte, error)
	// Decrypt data using data key
	decryptData([]byte, []byte) ([]byte, error)

	// Seal takes the message, performs the encryption and return
	// the encrypted data along with the wrapped data
	// The encoded message contains the first byte as size of the wrapped key,
	// the wrapped key and finally the encrypted message.

	// |  byte 0  |   byte 1   |   byte 2   |    ...   | byte (n +1) |    byte (n+2)  |  ... | byte (n + m + 2) |
	// |----------|------------|------------|----------|--------------|----------------|------|------------------|
	// | key size | key byte 0 | key byte 1 |      ... | key byte n   | message byte 0 |  ... |  message byte m  |

	Seal([]byte) ([]byte, error)
	// Read takes cipher text, performs the decryption and return
	// the plaintext data
	// The incoming byte array has the following structure:
	// The first byte indicates the size of the wrapped key.
	// The n next bytes contain the wrapped ky itself (n is the value of the first byte)
	// The remaining bytes are the message itself.

	// |  byte 0  |   byte 1   |   byte 2   |    ...   | byte (n +1) |    byte (n+2)  |  ... | byte (n + m + 2) |
	// |----------|------------|------------|----------|--------------|----------------|------|------------------|
	// | key size | key byte 0 | key byte 1 |      ... | key byte n   | message byte 0 |  ... |  message byte m  |

	Read([]byte) ([]byte, error)
}
