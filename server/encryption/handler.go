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
	EncryptionKeyLength int = 24
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
	defaultDKS   []byte
	keywrappeder *subtle.KWP
	blockCypher  *cipher.AEAD
}

// NewLocalEncriptionHandler generates a new instance of LocalEncryptionHandler
func NewLocalEncriptionHandler() (*LocalEncryptionHandler, error) {
	localEncryptionHandler := LocalEncryptionHandler{}

	// generate default DKS

	defaultDKS, err := localEncryptionHandler.generateDKS()
	localEncryptionHandler.defaultDKS = defaultDKS

	// Init key wrapper

	masterKeyStr := os.Getenv(masterKeyVarName)
	masterKey := []byte(masterKeyStr)
	kwp, err := subtle.NewKWP(masterKey)

	if err != nil {
		return nil, err
	}
	localEncryptionHandler.keywrappeder = kwp

	// Init cypher in GCM

	block, err := aes.NewCipher(localEncryptionHandler.defaultDKS)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)

	if err != nil {
		return nil, err
	}
	localEncryptionHandler.blockCypher = &gcm

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
	// use Tinker to wrapped Data Key
	// https://github.com/google/tink/commit/22467ef7273d73b2d65e4b50310aab4af006bb7e
	wrappededKey, err := handler.keywrappeder.Wrap(dks)

	if err != nil {
		return nil, err
	}

	return wrappededKey, nil

}

func (handler *LocalEncryptionHandler) encryptData(dks []byte, plaintextData []byte) ([]byte, error) {
	gcm := *handler.blockCypher
	// init nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		panic(err.Error())
	}
	ciphertext := gcm.Seal(nil, nonce, plaintextData, nil)
	return ciphertext, nil
}
