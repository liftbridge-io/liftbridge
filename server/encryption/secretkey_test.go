package encryption

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateDataKeyCorrectly(t *testing.T) {

	// Given a key handler
	keyHandler := LocalEncryptionHandler{}

	// Generate DKS
	dks, err := keyHandler.generateDKS()
	// Expect retrieval without error
	require.NoError(t, err)
	require.Equal(t, len(dks), EncryptionKeyLength)
}
