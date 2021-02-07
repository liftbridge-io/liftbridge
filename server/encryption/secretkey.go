package encryption

import (
	"errors"
	"os"
)

const (
	// EnvironmentKeyType indicates that the encryption key
	// is present in the environment variables.
	EnvironmentKeyType int = 0
)

var (
	encryptionKey = "ENV_ENCRYPTION_KEY"
	//ErrEncryptionKeyNotFound indicates that the encryption key
	// is not set in the environment variables
	ErrEncryptionKeyNotFound = errors.New("encryption key is not set")
)

// SecretKeyHandler provides the necessary method to safely retrieve
// secret encryption key to encrypt/decrypt data at rest
type SecretKeyHandler interface {
	RetrieveSecretKey() (string, error)
}

// EnvKeyHandler provides functionalities to load secret key
// from environment variables
type EnvKeyHandler struct {
	keyName string
}

// RetrieveSecretKey retrieves the pre-configurated encryption key
// from the environment variables.
func (EnvKeyHandler) RetrieveSecretKey() (string, error) {
	encryptionKey = os.Getenv(encryptionKey)
	if encryptionKey == "" {
		return "", ErrEncryptionKeyNotFound
	}
	return encryptionKey, nil

}

// NewEnvKeyHandler initiate a new handler to retrieve key
// from environement variables
func NewEnvKeyHandler() EnvKeyHandler {
	return EnvKeyHandler{keyName: encryptionKey}
}

//KeyHandlerFactory generates the appropriate key handler
// At this moment, only handler to retrieve key from
// environment variables is provided.
func KeyHandlerFactory(keyType int) (SecretKeyHandler, error) {
	if keyType == EnvironmentKeyType {
		return NewEnvKeyHandler(), nil
	}
	return nil, errors.New("No encryption key handler type is provided")
}
