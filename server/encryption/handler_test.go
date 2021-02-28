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
