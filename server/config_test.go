package server

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	client "github.com/liftbridge-io/liftbridge-api/go"
	proto "github.com/liftbridge-io/liftbridge/server/protocol"
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
	require.True(t, config.LogNATS)
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
	require.Equal(t, false, config.Streams.ConcurrencyControl)

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
	require.Equal(t, int64(1024), config.Clustering.ReplicationMaxBytes)

	require.Equal(t, true, config.ActivityStream.Enabled)
	require.Equal(t, time.Minute, config.ActivityStream.PublishTimeout)
	require.Equal(t, client.AckPolicy_LEADER, config.ActivityStream.PublishAckPolicy)

	require.True(t, config.EmbeddedNATS)
	require.Equal(t, "nats.conf", config.EmbeddedNATSConfig)
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
	// Liftbridge TLS
	require.Equal(t, "./configs/certs/server.key", config.TLSKey)
	require.Equal(t, "./configs/certs/server.crt", config.TLSCert)
}

func TestNewConfigNATSTLS(t *testing.T) {
	config, err := NewConfig("configs/tls-nats.yaml")
	require.NoError(t, err)
	// NATS TLS
	// Parse test TLS
	cert, err := tls.LoadX509KeyPair("./configs/certs/server.crt", "./configs/certs/server.key")
	require.NoError(t, err)
	// CARoot parsing
	// Load CA cert
	caCert, err := ioutil.ReadFile("./configs/certs/caroot.pem")
	require.NoError(t, err)

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		RootCAs:      caCertPool,
	}
	require.Len(t, config.NATS.TLSConfig.Certificates, 1)
	require.Equal(t, tlsConfig.Certificates[0], config.NATS.TLSConfig.Certificates[0])
	require.NotNil(t, tlsConfig.RootCAs)
	require.Equal(t, tlsConfig.MinVersion, config.NATS.TLSConfig.MinVersion)
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

// Ensure custom StreamConfig can be applied correctly. If a given value is
// present in the StreamConfig it should be set. Otherwise, default values
// should be kept.
func TestStreamsConfigApplyOverrides(t *testing.T) {
	// Given custom stream config duration configuration is in milliseconds.
	customStreamConfig := &proto.StreamConfig{
		SegmentMaxBytes:               &proto.NullableInt64{Value: 1024},
		SegmentMaxAge:                 &proto.NullableInt64{Value: 1000000},
		RetentionMaxBytes:             &proto.NullableInt64{Value: 2048},
		RetentionMaxMessages:          &proto.NullableInt64{Value: 1000},
		RetentionMaxAge:               &proto.NullableInt64{Value: 1000000},
		CleanerInterval:               &proto.NullableInt64{Value: 1000000},
		CompactMaxGoroutines:          &proto.NullableInt32{Value: 10},
		AutoPauseTime:                 &proto.NullableInt64{Value: 1000000},
		AutoPauseDisableIfSubscribers: &proto.NullableBool{Value: true},
		MinIsr:                        &proto.NullableInt32{Value: 11},
		OptimisticConcurrencyControl:  &proto.NullableBool{Value: true},
	}
	streamConfig := StreamsConfig{}

	streamConfig.ApplyOverrides(customStreamConfig)

	s, _ := time.ParseDuration("1000s")

	// Expect custom stream config overwrites default stream config
	require.Equal(t, int64(1024), streamConfig.SegmentMaxBytes)
	require.Equal(t, s, streamConfig.SegmentMaxAge)
	require.Equal(t, int64(2048), streamConfig.RetentionMaxBytes)
	require.Equal(t, int64(1000), streamConfig.RetentionMaxMessages)
	require.Equal(t, s, streamConfig.RetentionMaxAge)
	require.Equal(t, s, streamConfig.CleanerInterval)
	require.Equal(t, 10, streamConfig.CompactMaxGoroutines)
	require.Equal(t, s, streamConfig.AutoPauseTime)
	require.True(t, streamConfig.AutoPauseDisableIfSubscribers)
	require.Equal(t, 11, streamConfig.MinISR)
	require.Equal(t, true, streamConfig.ConcurrencyControl)
}

// Ensure default stream configs are always present. This should be the case
// when custom stream configs are not set.
func TestStreamsConfigApplyOverridesDefault(t *testing.T) {
	s, _ := time.ParseDuration("1000s")
	autoPauseTime := 30 * time.Second

	// Given a default stream config
	streamConfig := StreamsConfig{
		SegmentMaxBytes: 2048,
		SegmentMaxAge:   s,
		AutoPauseTime:   autoPauseTime,
		MinISR:          2,
	}

	// Given custom configs
	customStreamConfig := &proto.StreamConfig{
		RetentionMaxBytes:    &proto.NullableInt64{Value: 1024},
		RetentionMaxMessages: &proto.NullableInt64{Value: 1000},
		RetentionMaxAge:      &proto.NullableInt64{Value: 1000000},
		CleanerInterval:      &proto.NullableInt64{Value: 1000000},
		CompactMaxGoroutines: &proto.NullableInt32{Value: 10},
	}

	streamConfig.ApplyOverrides(customStreamConfig)

	// Ensure that in case of non-overlap values, default configs
	// remain present
	require.Equal(t, int64(2048), streamConfig.SegmentMaxBytes)
	require.Equal(t, s, streamConfig.SegmentMaxAge)
	require.Equal(t, autoPauseTime, streamConfig.AutoPauseTime)
	require.Equal(t, 2, streamConfig.MinISR)
	require.Equal(t, false, streamConfig.ConcurrencyControl)

	// Ensure values from custom configs overwrite default configs
	require.Equal(t, int64(1024), streamConfig.RetentionMaxBytes)
	require.Equal(t, int64(1000), streamConfig.RetentionMaxMessages)
	require.Equal(t, s, streamConfig.RetentionMaxAge)
	require.Equal(t, s, streamConfig.CleanerInterval)
	require.Equal(t, 10, streamConfig.CompactMaxGoroutines)

}

// Ensure compact activation is correctly parsed.
func TestStreamsConfigApplyOverridesCompactEnabled(t *testing.T) {
	// Given a default stream config
	streamConfig := StreamsConfig{}

	// Given custom configs with option to disable compact
	customStreamConfig := &proto.StreamConfig{
		CompactEnabled: &proto.NullableBool{Value: false},
	}

	streamConfig.ApplyOverrides(customStreamConfig)

	// Ensure that stream config correctly disable compact option
	require.Equal(t, false, streamConfig.Compact)

	// Given a default stream config
	streamConfig2 := StreamsConfig{}
	// Given custom configs with option to enable compact
	customStreamConfig2 := &proto.StreamConfig{
		CompactEnabled: &proto.NullableBool{Value: true},
	}

	streamConfig2.ApplyOverrides(customStreamConfig2)

	// Ensure that stream config correctly enable compact option
	require.Equal(t, true, streamConfig2.Compact)

	// Given a default stream config with default compaction disabled
	streamConfig3 := StreamsConfig{}

	// Given custom configs with NO option to configure compact
	customStreamConfig3 := &proto.StreamConfig{}

	streamConfig3.ApplyOverrides(customStreamConfig3)

	// Ensure that stream default config is retained (by default
	// compact.enabled is set to true)
	require.Equal(t, true, streamConfig2.Compact)
}
