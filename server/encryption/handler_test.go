package encryption

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Ensure that a data key can be generated
func TestGenerateDataKeyCorrectly(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LOCAL_MASTER_KEY", "+KbPeShVmYq3t6w9")

	// Given a key handler
	keyHandler, err := NewLocalEncriptionHandler()
	require.NoError(t, err)

	// Generate DKS
	dks, err := keyHandler.generateDKS()

	// Expect DKS is generated without error
	require.NoError(t, err)
	require.NotEmpty(t, dks)
}

// ENsure that key can be wrapped by using master key
func TestWrapDataKeyCorrectly(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LOCAL_MASTER_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncriptionHandler()
	require.NoError(t, err)

	// Generate DKS
	dks, err := keyHandler.generateDKS()

	// Expect DKS is generated without error
	require.NoError(t, err)
	require.Equal(t, EncryptionKeyLength, len(dks))

	// Start wrapping DKS
	wrappedDKS, err := keyHandler.wrapDKS(dks)

	// Expect DKS is wrapped without error
	require.NoError(t, err)
	require.NotEmpty(t, wrappedDKS)
	// Expect wrapped DKS should not be the same
	// as plain text DKS
	require.NotEqual(t, dks, wrappedDKS)
}

// Ensure that key can be unwrapped by using master key
func TestUnWrapDataKeyCorrectly(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LOCAL_MASTER_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncriptionHandler()
	require.NoError(t, err)

	// Generate DKS
	dks, err := keyHandler.generateDKS()

	// Expect DKS is generated without error
	require.NoError(t, err)
	require.Equal(t, EncryptionKeyLength, len(dks))

	// Start wrapping DKS
	wrappedDKS, err := keyHandler.wrapDKS(dks)
	require.NoError(t, err)

	// Unwrap
	key, err := keyHandler.unwrapDKS(wrappedDKS)
	require.NoError(t, err)

	// Expect the unwrapped key is actually correct
	require.Equal(t, dks, key)
}

// Ensure encryptions can encrypt the message
func TestEncryption(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LOCAL_MASTER_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncriptionHandler()
	require.NoError(t, err)

	// Given sample data
	plaintext := []byte("exampleplaintext")

	// Given a sample data key

	dks, err := keyHandler.generateDKS()
	require.NoError(t, err)

	ciphertext, err := keyHandler.encryptData(dks, plaintext)
	require.NoError(t, err)

	require.NotEmpty(t, ciphertext)

	// Expect cipher  text should not be the same
	// as plain text
	require.NotEqual(t, ciphertext, plaintext)
}

// Ensure that the decryption retrieves the same text after encryption
func TestDecryption(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LOCAL_MASTER_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncriptionHandler()
	require.NoError(t, err)

	// Given sample data
	plaintext := []byte("exampleplaintext")

	// Given a sample data key
	dks, err := keyHandler.generateDKS()
	require.NoError(t, err)

	// Encrpyt
	ciphertext, err := keyHandler.encryptData(dks, plaintext)
	require.NoError(t, err)

	// Decrypt
	decryptedText, err := keyHandler.decryptData(dks, ciphertext)
	require.NoError(t, err)

	// Expect to retrieve the same original text
	require.Equal(t, plaintext, decryptedText)
}

// Ensure that the data data encryption process can be performed
func TestSeal(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LOCAL_MASTER_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncriptionHandler()
	require.NoError(t, err)

	// Given sample data
	plaintext := []byte("exampleplaintext")

	// Cipher
	data, err := keyHandler.Seal(plaintext)
	require.NoError(t, err)

	// decompose key and cipher text
	// first  byte is the key size
	keySize := int(data[0])
	keyEndPos := keySize + 1
	wrappedKey := data[1:keyEndPos]
	encryptedData := data[keyEndPos:]

	// Expect that  a default DKS key is generated
	require.NotNil(t, keyHandler.defaultDKS)

	// Expect that the data is encrypted
	require.NotEqual(t, encryptedData, plaintext)

	// Expect that the DKS key is wrapped
	require.NotEqual(t, wrappedKey, keyHandler.defaultDKS)

}

// Ensure that the data data decryption process can be performed
func TestRead(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LOCAL_MASTER_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncriptionHandler()
	require.NoError(t, err)

	// Given sample data
	plaintext := []byte("exampleplaintext")

	// Cipher
	data, err := keyHandler.Seal(plaintext)
	require.NoError(t, err)

	// Decipher
	deciphertext, err := keyHandler.Read(data)
	require.NoError(t, err)

	// Expect the message is deciphered correctly
	require.Equal(t, plaintext, deciphertext)

}
