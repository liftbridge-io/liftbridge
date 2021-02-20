package encryption

import (
	"crypto/rand"
)

const (
	// EncryptionKeyLength provides the length for data key
	EncryptionKeyLength int = 128
)

var (
	masterKeyVarName = "LOCAL_MASTER_KEY"
)

// Handler provides the necessary method to safely retrieve
// secret encryption key to encrypt/decrypt data at rest
type Handler interface {
	generateDKS() ([]byte, error)
	wrapDKS(string) (string, error)
	ecnryptData(string, string) (string, error)
	decryptData(string, string) (string, error)
}

// LocalEncryptionHandler provides functionalities to load secret key
// from environment variables
type LocalEncryptionHandler struct {
}

// generateDKS retrieves the pre-configurated encryption key
// from the environment variables.
func (LocalEncryptionHandler) generateDKS() ([]byte, error) {
	key := make([]byte, EncryptionKeyLength)

	_, err := rand.Read(key)

	if err != nil {
		return nil, err
	}

	return key, nil

}

// use Tinker to wrap Data Key
// https://github.com/google/tink/commit/22467ef7273d73b2d65e4b50310aab4af006bb7e
