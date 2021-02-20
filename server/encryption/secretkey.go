package encryption

import (
	"crypto/rand"
)

const (
	// EnvironmentKeyType indicates that the encryption key
	// is present in the environment variables.
	EnvironmentKeyType int = 0
)

var (
	masterKeyVarName = "LOCAL_MASTER_KEY"
)

// EncryptionHandler provides the necessary method to safely retrieve
// secret encryption key to encrypt/decrypt data at rest
type EncryptionHandler interface {
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
	key := make([]byte, 128)

	_, err := rand.Read(key)

	if err != nil {
		return nil, err
	}

	return key, nil

}
