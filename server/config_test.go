package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Ensure NewConfig properly parses config files.
func TestNewConfig(t *testing.T) {
	config, err := NewConfig("configs/full.yaml")
	require.NoError(t, err)

	require.Equal(t, "0.0.0.0", config.Host)
	require.Equal(t, 5050, config.Port)
	require.Equal(t, uint32(5), config.LogLevel)
	require.True(t, config.LogRecovery)
	require.Equal(t, "/foo", config.DataDir)
	require.Equal(t, 10, config.BatchMaxMessages)
	require.Equal(t, time.Second, config.BatchWaitTime)
	require.Equal(t, time.Minute, config.MetadataCacheMaxAge)

	require.Equal(t, int64(1024), config.Log.RetentionMaxBytes)
	require.Equal(t, int64(100), config.Log.RetentionMaxMessages)
	require.Equal(t, time.Hour, config.Log.RetentionMaxAge)
	require.Equal(t, time.Minute, config.Log.CleanerInterval)
	require.Equal(t, int64(64), config.Log.SegmentMaxBytes)
	require.Equal(t, time.Minute, config.Log.LogRollTime)
	require.True(t, config.Log.Compact)
	require.Equal(t, 2, config.Log.CompactMaxGoroutines)

	require.Equal(t, "foo", config.Clustering.ServerID)
	require.Equal(t, "bar", config.Clustering.Namespace)
	require.Equal(t, 10, config.Clustering.RaftSnapshots)
	require.Equal(t, uint64(100), config.Clustering.RaftSnapshotThreshold)
	require.Equal(t, 5, config.Clustering.RaftCacheSize)
	require.Equal(t, []string{"a", "b"}, config.Clustering.RaftBootstrapPeers)
	require.True(t, config.Clustering.RaftLogging)
	require.Equal(t, time.Minute, config.Clustering.ReplicaMaxLagTime)
	require.Equal(t, 30*time.Second, config.Clustering.ReplicaMaxLeaderTimeout)
	require.Equal(t, 2*time.Second, config.Clustering.ReplicaMaxIdleWait)
	require.Equal(t, 3*time.Second, config.Clustering.ReplicaFetchTimeout)
	require.Equal(t, 1, config.Clustering.MinISR)

	require.Equal(t, []string{"nats://localhost:4222"}, config.NATS.Servers)
}

//Ensure that default config is loaded
func TestDefaultConfig(t *testing.T) {
	config, err := NewConfig("")
	require.NoError(t, err)
	require.Equal(t, 512, config.Clustering.RaftCacheSize)
	require.Equal(t, "liftbridge-default", config.Clustering.Namespace)
	require.Equal(t, 1024, config.BatchMaxMessages)
}

// Ensure we can properly parse NATS username and password from a config file.
func TestNewConfigNATSAuth(t *testing.T) {
	config, err := NewConfig("configs/nats_auth.yaml")
	require.NoError(t, err)
	require.Equal(t, "admin", config.NATS.User)
	require.Equal(t, "password", config.NATS.Password)
}

// Ensure parsing host and listen
func TestNewConfigListen(t *testing.T) {
	config, err := NewConfig("configs/listen-host.yaml")
	require.NoError(t, err)
	require.Equal(t, "192.168.0.1", config.Listen.Host)
	require.Equal(t, int(4222), config.Listen.Port)
	require.Equal(t, "my-host", config.Host)
	require.Equal(t, int(4333), config.Port)
}

// Ensure parsing TLS config
func TestNewConfigTLS(t *testing.T) {
	config, err := NewConfig("configs/tls.yaml")
	require.NoError(t, err)
	require.Equal(t, "./configs/certs/server.key", config.TLSKey)
	require.Equal(t, "./configs/certs/server.crt", config.TLSCert)
}
