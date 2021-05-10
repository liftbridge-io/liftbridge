package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"os"

	"github.com/google/tink/go/kwp/subtle"
)

type AESKeyLength int

const (
	// DataKeyLength provides the length for data key in bytes.
	// It is recommended to use an authentication key with 32 or 64 bytes.
	// The data key length must be either
	// 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256 modes.
	AES256KeyLength AESKeyLength = 32
	AES192KeyLength AESKeyLength = 24
	AES128KeyLength AESKeyLength = 16
)

var (
	masterKeyVarName = "LIFTBRIDGE_ENCRYPTION_KEY"
)

// LocalEncryptionHandler provides functionalities to load secret key
// from environment variables
type LocalEncryptionHandler struct {
	defaultDEK  []byte
	keyWrapper  *subtle.KWP
	blockCipher *cipher.AEAD
}

// NewLocalEncryptionHandler generates a new instance of LocalEncryptionHandler.
func NewLocalEncryptionHandler() (*LocalEncryptionHandler, error) {
	localEncryptionHandler := LocalEncryptionHandler{}

	// Init key wrapper

	masterKeyStr := os.Getenv(masterKeyVarName)
	masterKey := []byte(masterKeyStr)
	kwp, err := subtle.NewKWP(masterKey)

	if err != nil {
		return nil, err
	}
	localEncryptionHandler.keyWrapper = kwp

	return &localEncryptionHandler, nil
}

// generateDEK generate a random AES data encryption key
func (handler *LocalEncryptionHandler) generateDEK() ([]byte, error) {
	key := make([]byte, AES256KeyLength)

	if _, err := rand.Read(key); err != nil {
		return nil, err
	}

	return key, nil

}

func (handler *LocalEncryptionHandler) wrapDEK(dek []byte) ([]byte, error) {
	// use Tinker to wrap data key
	// https://github.com/google/tink/commit/22467ef7273d73b2d65e4b50310aab4af006bb7e
	wrappedKey, err := handler.keyWrapper.Wrap(dek)

	if err != nil {
		return nil, err
	}

	return wrappedKey, nil
}

func (handler *LocalEncryptionHandler) unwrapDEK(wrappedDEK []byte) ([]byte, error) {
	// use Tinker to unwrap data key
	// https://github.com/google/tink/commit/22467ef7273d73b2d65e4b50310aab4af006bb7e
	key, err := handler.keyWrapper.Unwrap(wrappedDEK)

	if err != nil {
		return nil, err
	}

	return key, nil
}

func (handler *LocalEncryptionHandler) encryptData(dek []byte, plaintextData []byte) ([]byte, error) {
	// init cipher in GCM
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)

	if err != nil {
		return nil, err
	}
	// init nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nonce, nonce, plaintextData, nil)
	return ciphertext, nil
}

func (handler *LocalEncryptionHandler) decryptData(dek []byte, encryptedData []byte) ([]byte, error) {
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)

	if err != nil {
		return nil, err
	}

	// get nonce
	nonceSize := gcm.NonceSize()
	nonce, ciphertext := encryptedData[:nonceSize], encryptedData[nonceSize:]

	// decrypt the data
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// Seal takes the message, performs the encryption and return
// the encrypted data along with the wrapped data
// The encoded message contains the first byte as size of the wrapped key,
// the wrapped key and finally the encrypted message.

// |  byte 0  |   byte 1   |   byte 2   |    ...   | byte (n +1 ) |    byte (n+2)  |  ... | byte (n + m + 2) |
// |----------|------------|------------|----------|--------------|----------------|------|------------------|
// | key size | key byte 0 | key byte 1 |      ... | key byte n   | message byte 0 |  ... |  message byte m  |

func (handler *LocalEncryptionHandler) Seal(data []byte) ([]byte, error) {
	// Generate a default Data Key (DEK) if not yet available
	if handler.defaultDEK == nil {
		dekKey, err := handler.generateDEK()

		if err != nil {
			return nil, err
		}
		handler.defaultDEK = dekKey
	}

	// encrypt the message
	ciphertext, err := handler.encryptData(handler.defaultDEK, data)
	if err != nil {
		return nil, err
	}

	// wrap the data key

	wrappedKey, err := handler.wrapDEK(handler.defaultDEK)
	if err != nil {
		return nil, err
	}

	keyLength := len(wrappedKey)

	// concatenate:  key size | wrapped key | ciphertext

	keySize := []byte{byte(keyLength)}
	// allocate data sequence
	dataSequenceSize := len(keySize) + keyLength + len(ciphertext)
	dataSequence := make([]byte, dataSequenceSize)

	// copy key size, wrapped key and cipher text to data sequence
	copy(dataSequence[:1], keySize)
	copy(dataSequence[1:keyLength+1], wrappedKey)
	copy(dataSequence[keyLength+1:], ciphertext)

	return dataSequence, nil
}

// Read takes cipher text, performs the decryption and return
// the plaintext data
// The incoming byte array has the following structure:
// The first byte indicates the size of the wrapped key.
// The n next bytes contain the wrapped ky itself (n is the value of the first byte)
// The remaining bytes are the message itself.

// |  byte 0  |   byte 1   |   byte 2   |    ...   | byte (n +1 ) |    byte (n+2)  |  ... | byte (n + m + 2) |
// |----------|------------|------------|----------|--------------|----------------|------|------------------|
// | key size | key byte 0 | key byte 1 |      ... | key byte n   | message byte 0 |  ... |  message byte m  |

func (handler *LocalEncryptionHandler) Read(encryptedData []byte) ([]byte, error) {
	// Decompose wrapped key and cypher text
	keySize := int(encryptedData[0])
	keyEndPos := keySize + 1
	wrappedDEK := encryptedData[1:keyEndPos]

	ciphertext := encryptedData[keyEndPos:]
	unwrappedDEK, err := handler.unwrapDEK(wrappedDEK)

	if err != nil {
		return nil, err
	}

	// Decipher the message

	plaintext, err := handler.decryptData(unwrappedDEK, ciphertext)

	if err != nil {
		return nil, err
	}
	return plaintext, nil
}
