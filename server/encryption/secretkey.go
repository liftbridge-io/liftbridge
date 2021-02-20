package encryption

import (
	"crypto/rand"
	"os"

	"github.com/google/tink/go/kwp/subtle"
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
	keywrappeder *subtle.KWP
}

// NewLocalEncriptionHandler generates a new instance of LocalEncryptionHandler
func NewLocalEncriptionHandler() (*LocalEncryptionHandler, error) {
	localEncryptionHandler := LocalEncryptionHandler{}
	masterKeyStr := os.Getenv(masterKeyVarName)
	masterKey := []byte(masterKeyStr)
	kwp, err := subtle.NewKWP(masterKey)
	if err != nil {
		return nil, err
	}
	localEncryptionHandler.keywrappeder = kwp
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

	wrappededKey, err := handler.keywrappeder.Wrap(dks)

	if err != nil {
		return nil, err
	}

	return wrappededKey, nil

}

// use Tinker to wrapped Data Key
// https://github.com/google/tink/commit/22467ef7273d73b2d65e4b50310aab4af006bb7e
