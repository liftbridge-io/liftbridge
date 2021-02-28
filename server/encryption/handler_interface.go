package encryption

// Handler provides the necessary method to safely retrieve
// secret encryption key to encrypt/decrypt data at rest
type Handler interface {
	generateDKS() ([]byte, error)
	wrapDKS([]byte) ([]byte, error)
	ecnryptData([]byte, []byte) ([]byte, error)
	decryptData([]byte, []byte) ([]byte, error)
	Seal([]byte) ([]byte, []byte, error)
	Read([]byte, []byte) ([]byte, error)
}
