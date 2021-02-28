package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"os"

	"github.com/google/tink/go/kwp/subtle"
)

const (
	// EncryptionKeyLength provides the length for data key in bytes
	// It is recommended to use an authentication key with 32 or 64 bytes.
	// The encryption key, if set, must be either
	// 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256 modes.
	EncryptionKeyLength int = 32
)

var (
	masterKeyVarName = "LOCAL_MASTER_KEY"
)

// LocalEncryptionHandler provides functionalities to load secret key
// from environment variables
type LocalEncryptionHandler struct {
	defaultDKS  []byte
	keywrapper  *subtle.KWP
	blockCypher *cipher.AEAD
}

// NewLocalEncriptionHandler generates a new instance of LocalEncryptionHandler
func NewLocalEncriptionHandler() (*LocalEncryptionHandler, error) {
	localEncryptionHandler := LocalEncryptionHandler{}

	// Init key wrapper

	masterKeyStr := os.Getenv(masterKeyVarName)
	masterKey := []byte(masterKeyStr)
	kwp, err := subtle.NewKWP(masterKey)

	if err != nil {
		return nil, err
	}
	localEncryptionHandler.keywrapper = kwp

	return &localEncryptionHandler, nil
}

// generateDKS retrieves the pre-configurated encryption key
// from the environment variables.
func (handler *LocalEncryptionHandler) generateDKS() ([]byte, error) {
	key := make([]byte, EncryptionKeyLength)

	_, err := rand.Read(key)

	if err != nil {
		return nil, err
	}

	return key, nil

}

func (handler *LocalEncryptionHandler) wrapDKS(dks []byte) ([]byte, error) {
	// use Tinker to wrap Data Key
	// https://github.com/google/tink/commit/22467ef7273d73b2d65e4b50310aab4af006bb7e
	wrappededKey, err := handler.keywrapper.Wrap(dks)

	if err != nil {
		return nil, err
	}

	return wrappededKey, nil

}

func (handler *LocalEncryptionHandler) unwrapDKS(wrappedDKS []byte) ([]byte, error) {
	// use Tinker to unwrap Data Key
	// https://github.com/google/tink/commit/22467ef7273d73b2d65e4b50310aab4af006bb7e
	key, err := handler.keywrapper.Unwrap(wrappedDKS)

	if err != nil {
		return nil, err
	}

	return key, nil
}

func (handler *LocalEncryptionHandler) encryptData(dks []byte, plaintextData []byte) ([]byte, error) {
	// Init cypher in GCM

	block, err := aes.NewCipher(dks)
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
		panic(err.Error())
	}
	ciphertext := gcm.Seal(nonce, nonce, plaintextData, nil)
	return ciphertext, nil
}

func (handler *LocalEncryptionHandler) decryptData(dks []byte, encryptedData []byte) ([]byte, error) {

	block, err := aes.NewCipher(dks)
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

	//Decrypt the data

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// Seal takes data key and message, performs the encryption and return
// the encrypted data along with the wrapped data
func (handler *LocalEncryptionHandler) Seal(data []byte) ([]byte, []byte, error) {
	// Generate a default Data Key (DKS) if not yet available
	if handler.defaultDKS == nil {
		dksKey, err := handler.generateDKS()

		if err != nil {
			return nil, nil, err
		}
		handler.defaultDKS = dksKey
	}

	// Cipher the message
	ciphertext, err := handler.encryptData(handler.defaultDKS, data)
	if err != nil {
		return nil, nil, err
	}

	// wrap the data key

	wrappedKey, err := handler.wrapDKS(handler.defaultDKS)
	if err != nil {
		return nil, nil, err
	}

	return ciphertext, wrappedKey, nil
}

// Read takes cipher text along with a wrapped data key, performs the decryption and return
// the plaintext data
func (handler *LocalEncryptionHandler) Read(ciphertext []byte, wrappedDKS []byte) ([]byte, error) {
	unwrappedDKS, err := handler.unwrapDKS(wrappedDKS)

	if err != nil {
		return nil, err
	}

	// Decipher the message

	plaintext, err := handler.decryptData(unwrappedDKS, ciphertext)

	if err != nil {
		return nil, err
	}
	return plaintext, nil
}
