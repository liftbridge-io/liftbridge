package encryption

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Ensure that a data key can be generated
func TestGenerateDataKeyCorrectly(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LIFTBRIDGE_ENCRYPTION_KEY", "+KbPeShVmYq3t6w9")

	// Given a key handler
	keyHandler, err := NewLocalEncryptionHandler()
	require.NoError(t, err)

	// Generate DEK
	dek, err := keyHandler.generateDEK()

	// Expect DEK is generated without error
	require.NoError(t, err)
	require.NotEmpty(t, dek)
}

// Ensure that the data key can be wrapped with the master key.
func TestWrapDataKeyCorrectly(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LIFTBRIDGE_ENCRYPTION_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncryptionHandler()
	require.NoError(t, err)

	// Generate DEK
	dek, err := keyHandler.generateDEK()

	// Expect DEK is generated without error
	require.NoError(t, err)
	require.Equal(t, 32, len(dek))

	// Start wrapping DEK
	wrappedDEK, err := keyHandler.wrapDEK(dek)

	// Expect DEK is wrapped without error
	require.NoError(t, err)
	require.NotEmpty(t, wrappedDEK)
	// Expect wrapped DEK should not be the same
	// as plain text DEK
	require.NotEqual(t, dek, wrappedDEK)
}

// Ensure that the data key can be unwrapped using the master key.
func TestUnWrapDataKeyCorrectly(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LIFTBRIDGE_ENCRYPTION_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncryptionHandler()
	require.NoError(t, err)

	// Generate DEK
	dek, err := keyHandler.generateDEK()

	// Expect DEK is generated without error
	require.NoError(t, err)
	require.Equal(t, 32, len(dek))

	// Start wrapping DEK
	wrappedDEK, err := keyHandler.wrapDEK(dek)
	require.NoError(t, err)

	// Unwrap
	key, err := keyHandler.unwrapDEK(wrappedDEK)
	require.NoError(t, err)

	// Expect the unwrapped key is actually correct
	require.Equal(t, dek, key)
}

// Ensure encryption of messages works correctly.
func TestEncryption(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LIFTBRIDGE_ENCRYPTION_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncryptionHandler()
	require.NoError(t, err)

	// Given sample data
	plaintext := []byte("exampleplaintext")

	// Given a sample data key

	dek, err := keyHandler.generateDEK()
	require.NoError(t, err)

	ciphertext, err := keyHandler.encryptData(dek, plaintext)
	require.NoError(t, err)

	require.NotEmpty(t, ciphertext)

	// expect cipher text should not be the same as plain text
	require.NotEqual(t, ciphertext, plaintext)
}

// Ensure that the decryption retrieves the same text after encryption.
func TestDecryption(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LIFTBRIDGE_ENCRYPTION_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncryptionHandler()
	require.NoError(t, err)

	// Given sample data
	plaintext := []byte("exampleplaintext")

	// Given a sample data key
	dek, err := keyHandler.generateDEK()
	require.NoError(t, err)

	// Encrpyt
	ciphertext, err := keyHandler.encryptData(dek, plaintext)
	require.NoError(t, err)

	// Decrypt
	decryptedText, err := keyHandler.decryptData(dek, ciphertext)
	require.NoError(t, err)

	// Expect to retrieve the same original text
	require.Equal(t, plaintext, decryptedText)
}

// Ensure that the data encryption process can be performed.
func TestSeal(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LIFTBRIDGE_ENCRYPTION_KEY", "t7w!z%C*F-JaNcRf")

	// Given a key handler
	keyHandler, err := NewLocalEncryptionHandler()
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

	// Expect that  a default DEK key is generated
	require.NotNil(t, keyHandler.defaultDEK)

	// Expect that the data is encrypted
	require.NotEqual(t, encryptedData, plaintext)

	// Expect that the DEK key is wrapped
	require.NotEqual(t, wrappedKey, keyHandler.defaultDEK)

}

// Ensure that the data decryption process can be performed.
func TestRead(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LIFTBRIDGE_ENCRYPTION_KEY", "/A?D(G+KbPdSgVkYp3s6v9y$B&E)H@Mc")

	// Given a key handler
	keyHandler, err := NewLocalEncryptionHandler()
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
