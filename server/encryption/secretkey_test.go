package encryption

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestWrapDataKeyCorrectly(t *testing.T) {
	// Set a random AES key as master key
	os.Setenv("LOCAL_MASTER_KEY", "+KbPeShVmYq3t6w9")

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
