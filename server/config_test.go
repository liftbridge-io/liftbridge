package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Ensure NewConfig properly parses config files.
func TestNewConfigFromFile(t *testing.T) {
	config, err := NewConfig("configs/full.yaml")
	require.NoError(t, err)

	require.Equal(t, "localhost", config.Listen.Host)
	require.Equal(t, 9293, config.Listen.Port)
	require.Equal(t, "0.0.0.0", config.Host)
	require.Equal(t, 5050, config.Port)
	require.Equal(t, uint32(5), config.LogLevel)
	require.True(t, config.LogRecovery)
	require.True(t, config.LogRaft)
	require.Equal(t, "/foo", config.DataDir)
	require.Equal(t, 10, config.BatchMaxMessages)
	require.Equal(t, time.Second, config.BatchMaxTime)
	require.Equal(t, time.Minute, config.MetadataCacheMaxAge)

	require.Equal(t, int64(1024), config.Streams.RetentionMaxBytes)
	require.Equal(t, int64(100), config.Streams.RetentionMaxMessages)
	require.Equal(t, time.Hour, config.Streams.RetentionMaxAge)
	require.Equal(t, time.Minute, config.Streams.CleanerInterval)
	require.Equal(t, int64(64), config.Streams.SegmentMaxBytes)
	require.Equal(t, time.Minute, config.Streams.SegmentMaxAge)
	require.True(t, config.Streams.Compact)
	require.Equal(t, 2, config.Streams.CompactMaxGoroutines)

	require.Equal(t, "foo", config.Clustering.ServerID)
	require.Equal(t, "bar", config.Clustering.Namespace)
	require.Equal(t, 10, config.Clustering.RaftSnapshots)
	require.Equal(t, uint64(100), config.Clustering.RaftSnapshotThreshold)
	require.Equal(t, 5, config.Clustering.RaftCacheSize)
	require.Equal(t, []string{"a", "b"}, config.Clustering.RaftBootstrapPeers)
	require.Equal(t, time.Minute, config.Clustering.ReplicaMaxLagTime)
	require.Equal(t, 30*time.Second, config.Clustering.ReplicaMaxLeaderTimeout)
	require.Equal(t, 2*time.Second, config.Clustering.ReplicaMaxIdleWait)
	require.Equal(t, 3*time.Second, config.Clustering.ReplicaFetchTimeout)
	require.Equal(t, 1, config.Clustering.MinISR)

	require.Equal(t, []string{"nats://localhost:4222"}, config.NATS.Servers)
	require.Equal(t, "user", config.NATS.User)
	require.Equal(t, "pass", config.NATS.Password)
}

// Ensure that default config is loaded.
func TestNewConfigDefault(t *testing.T) {
	config, err := NewConfig("")
	require.NoError(t, err)
	require.Equal(t, 512, config.Clustering.RaftCacheSize)
	require.Equal(t, "liftbridge-default", config.Clustering.Namespace)
	require.Equal(t, 1024, config.BatchMaxMessages)
}

// Ensure that both config file and default configs are loaded.
func TestNewConfigDefaultAndFile(t *testing.T) {
	config, err := NewConfig("configs/simple.yaml")
	require.NoError(t, err)
	// Ensure custom configs are loaded
	require.Equal(t, true, config.LogRecovery)
	require.Equal(t, int64(1024), config.Streams.RetentionMaxBytes)

	// Ensure also default values are loaded at the same time
	require.Equal(t, 512, config.Clustering.RaftCacheSize)
	require.Equal(t, "liftbridge-default", config.Clustering.Namespace)
	require.Equal(t, 1024, config.BatchMaxMessages)
}

// Ensure we can properly parse NATS username and password from a config file.
func TestNewConfigNATSAuth(t *testing.T) {
	config, err := NewConfig("configs/nats-auth.yaml")
	require.NoError(t, err)
	require.Equal(t, "admin", config.NATS.User)
	require.Equal(t, "password", config.NATS.Password)
}

// Ensure parsing host and listen.
func TestNewConfigListen(t *testing.T) {
	config, err := NewConfig("configs/listen-host.yaml")
	require.NoError(t, err)
	require.Equal(t, "192.168.0.1", config.Listen.Host)
	require.Equal(t, int(4222), config.Listen.Port)
	require.Equal(t, "my-host", config.Host)
	require.Equal(t, int(4333), config.Port)
}

// Ensure parsing TLS config.
func TestNewConfigTLS(t *testing.T) {
	config, err := NewConfig("configs/tls.yaml")
	require.NoError(t, err)
	require.Equal(t, "./configs/certs/server.key", config.TLSKey)
	require.Equal(t, "./configs/certs/server.crt", config.TLSCert)
}

// Ensure error is raised when given config file not found.
func TestNewConfigFileNotFound(t *testing.T) {
	_, err := NewConfig("somefile.yaml")
	require.Error(t, err)
}

// Ensure an error is returned when there is invalid configuration in listen.
func TestNewConfigInvalidClusteringSetting(t *testing.T) {
	_, err := NewConfig("configs/invalid-listen.yaml")
	require.Error(t, err)
}

// Ensure an error is returned when there is an unknown setting in the file.
func TestNewConfigUnknownSetting(t *testing.T) {
	_, err := NewConfig("configs/unknown-setting.yaml")
	require.Error(t, err)
}
