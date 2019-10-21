package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Ensure we can properly parse NATS username and password from a config file.
func TestConfigParseNATSAuth(t *testing.T) {
	config, err := NewConfig("configs/nats_auth.conf")
	require.NoError(t, err)
	require.Equal(t, "admin", config.NATS.User)
	require.Equal(t, "password", config.NATS.Password)
}
