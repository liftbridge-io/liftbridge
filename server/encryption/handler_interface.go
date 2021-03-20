package encryption

// Handler provides the necessary method to safely retrieve
// secret encryption key to encrypt/decrypt data at rest
type Handler interface {
	generateDKS() ([]byte, error)
	wrapDKS([]byte) ([]byte, error)
	encryptData([]byte, []byte) ([]byte, error)
	decryptData([]byte, []byte) ([]byte, error)
	Seal([]byte) ([]byte, error)
	Read([]byte) ([]byte, error)
}
