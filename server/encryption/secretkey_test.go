package encryption

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetrieveKeyFromEnvCorrectly(t *testing.T) {
	// Given that the key is configurated in the
	// environment variables

	os.Setenv(encryptionKey, "supersecret")
	// Given a key handler
	keyHandler, err := KeyHandlerFactory(EnvironmentKeyType)
	require.NoError(t, err)
	retrievedKey, err := keyHandler.RetrieveSecretKey()
	require.NoError(t, err)
	require.Equal(t, retrievedKey, "supersecret")
}

func TestEncryptionKeyNotSet(t *testing.T) {
	// Given that the key is not-configured d in the
	// environment variables

	os.Setenv(encryptionKey, "")
	// Given a key handler
	keyHandler, err := KeyHandlerFactory(EnvironmentKeyType)
	require.NoError(t, err)
	_, err = keyHandler.RetrieveSecretKey()
	require.Error(t, err)
}
