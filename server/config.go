package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hako/durafmt"
	client "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

const (
	// DefaultNamespace is the default cluster namespace to use if one is not
	// specified.
	DefaultNamespace = "liftbridge-default"

	// DefaultPort is the port to bind to if one is not specified.
	DefaultPort = 9292
)

// Config setting defaults.
const (
	defaultListenAddress                  = "0.0.0.0"
	defaultConnectionAddress              = "localhost"
	defaultReplicaMaxLagTime              = 15 * time.Second
	defaultReplicaMaxLeaderTimeout        = 15 * time.Second
	defaultReplicaMaxIdleWait             = 10 * time.Second
	defaultReplicationMaxBytes            = 1024 * 1024 // 1MB
	defaultRaftSnapshots                  = 2
	defaultRaftCacheSize                  = 512
	defaultMetadataCacheMaxAge            = 2 * time.Minute
	defaultBatchMaxMessages               = 1024
	defaultReplicaFetchTimeout            = 3 * time.Second
	defaultMinInsyncReplicas              = 1
	defaultRetentionMaxAge                = 7 * 24 * time.Hour
	defaultCleanerInterval                = 5 * time.Minute
	defaultMaxSegmentBytes                = 1024 * 1024 * 256 // 256MB
	defaultMaxSegmentAge                  = defaultRetentionMaxAge
	defaultActivityStreamPublishTimeout   = 5 * time.Second
	defaultActivityStreamPublishAckPolicy = client.AckPolicy_ALL
	defaultCursorsStreamReplicationFactor = maxReplicationFactor
	defaultCursorsStreamAutoPauseTime     = time.Minute
	defaultConcurrencyControl             = false
	defaultEncryption                     = false
	defaultGroupsConsumerTimeout          = 15 * time.Second
	defaultGroupsCoordinatorTimeout       = 15 * time.Second
)

// Config setting key names.
const (
	configListen              = "listen"
	configHost                = "host"
	configPort                = "port"
	configDataDir             = "data.dir"
	configMetadataCacheMaxAge = "metadata.cache.max.age"

	configLoggingLevel    = "logging.level"
	configLoggingRecovery = "logging.recovery"
	configLoggingRaft     = "logging.raft"
	configLoggingNATS     = "logging.nats"

	configBatchMaxMessages = "batch.max.messages"
	configBatchMaxTime     = "batch.max.time"

	configTLSKey                = "tls.key"
	configTLSCert               = "tls.cert"
	configTLSClientAuthEnabled  = "tls.client.auth.enabled"
	configTLSClientAuthCA       = "tls.client.auth.ca"
	configTLSClientAuthzEnabled = "tls.client.authz.enabled"
	configTLSClientAuthzModel   = "tls.client.authz.model"
	configTLSClientAuthzPolicy  = "tls.client.authz.policy"

	configNATSServers        = "nats.servers"
	configNATSUser           = "nats.user"
	configNATSPassword       = "nats.password"
	configNATSCert           = "nats.tls.cert"
	configNATSKey            = "nats.tls.key"
	configNATSCA             = "nats.tls.ca"
	configNATSEmbedded       = "nats.embedded"
	configNATSEmbeddedConfig = "nats.embedded.config"

	configStreamsRetentionMaxBytes             = "streams.retention.max.bytes"
	configStreamsRetentionMaxMessages          = "streams.retention.max.messages"
	configStreamsRetentionMaxAge               = "streams.retention.max.age"
	configStreamsCleanerInterval               = "streams.cleaner.interval"
	configStreamsSegmentMaxBytes               = "streams.segment.max.bytes"
	configStreamsSegmentMaxAge                 = "streams.segment.max.age"
	configStreamsCompactEnabled                = "streams.compact.enabled"
	configStreamsCompactMaxGoroutines          = "streams.compact.max.goroutines"
	configStreamsAutoPauseTime                 = "streams.auto.pause.time"
	configStreamsAutoPauseDisableIfSubscribers = "streams.auto.pause.disable.if.subscribers"
	configStreamsConcurrencyControl            = "streams.concurrency.control"
	configStreamsEncryption                    = "streams.encryption"

	configClusteringServerID                = "clustering.server.id"
	configClusteringNamespace               = "clustering.namespace"
	configClusteringRaftSnapshotRetain      = "clustering.raft.snapshot.retain"
	configClusteringRaftSnapshotThreshold   = "clustering.raft.snapshot.threshold"
	configClusteringRaftCacheSize           = "clustering.raft.cache.size"
	configClusteringRaftBootstrapSeed       = "clustering.raft.bootstrap.seed"
	configClusteringRaftBootstrapPeers      = "clustering.raft.bootstrap.peers"
	configClusteringRaftMaxQuorumSize       = "clustering.raft.max.quorum.size"
	configClusteringReplicaMaxLagTime       = "clustering.replica.max.lag.time"
	configClusteringReplicaMaxLeaderTimeout = "clustering.replica.max.leader.timeout"
	configClusteringReplicaMaxIdleWait      = "clustering.replica.max.idle.wait"
	configClusteringReplicaFetchTimeout     = "clustering.replica.fetch.timeout"
	configClusteringMinInsyncReplicas       = "clustering.min.insync.replicas"
	configClusteringReplicationMaxBytes     = "clustering.replication.max.bytes"

	configActivityStreamEnabled          = "activity.stream.enabled"
	configActivityStreamPublishTimeout   = "activity.stream.publish.timeout"
	configActivityStreamPublishAckPolicy = "activity.stream.publish.ack.policy"

	configCursorsStreamPartitions        = "cursors.stream.partitions"
	configCursorsStreamReplicationFactor = "cursors.stream.replication.factor"
	configCursorsStreamAutoPauseTime     = "cursors.stream.auto.pause.time"

	configGroupsConsumerTimeout    = "groups.consumer.timeout"
	configGroupsCoordinatorTimeout = "groups.coordinator.timeout"
)

var configKeys = map[string]struct{}{
	configListen:                               {},
	configHost:                                 {},
	configPort:                                 {},
	configDataDir:                              {},
	configMetadataCacheMaxAge:                  {},
	configLoggingLevel:                         {},
	configLoggingRecovery:                      {},
	configLoggingRaft:                          {},
	configLoggingNATS:                          {},
	configBatchMaxMessages:                     {},
	configBatchMaxTime:                         {},
	configTLSKey:                               {},
	configTLSCert:                              {},
	configTLSClientAuthEnabled:                 {},
	configTLSClientAuthCA:                      {},
	configTLSClientAuthzEnabled:                {},
	configTLSClientAuthzModel:                  {},
	configTLSClientAuthzPolicy:                 {},
	configNATSServers:                          {},
	configNATSUser:                             {},
	configNATSPassword:                         {},
	configNATSCert:                             {},
	configNATSKey:                              {},
	configNATSCA:                               {},
	configNATSEmbedded:                         {},
	configNATSEmbeddedConfig:                   {},
	configStreamsRetentionMaxBytes:             {},
	configStreamsRetentionMaxMessages:          {},
	configStreamsRetentionMaxAge:               {},
	configStreamsCleanerInterval:               {},
	configStreamsSegmentMaxBytes:               {},
	configStreamsSegmentMaxAge:                 {},
	configStreamsCompactEnabled:                {},
	configStreamsConcurrencyControl:            {},
	configStreamsEncryption:                    {},
	configStreamsCompactMaxGoroutines:          {},
	configStreamsAutoPauseTime:                 {},
	configStreamsAutoPauseDisableIfSubscribers: {},
	configClusteringServerID:                   {},
	configClusteringNamespace:                  {},
	configClusteringRaftSnapshotRetain:         {},
	configClusteringRaftSnapshotThreshold:      {},
	configClusteringRaftCacheSize:              {},
	configClusteringRaftBootstrapSeed:          {},
	configClusteringRaftBootstrapPeers:         {},
	configClusteringRaftMaxQuorumSize:          {},
	configClusteringReplicaMaxLagTime:          {},
	configClusteringReplicaMaxLeaderTimeout:    {},
	configClusteringReplicaMaxIdleWait:         {},
	configClusteringReplicaFetchTimeout:        {},
	configClusteringMinInsyncReplicas:          {},
	configClusteringReplicationMaxBytes:        {},
	configActivityStreamEnabled:                {},
	configActivityStreamPublishTimeout:         {},
	configActivityStreamPublishAckPolicy:       {},
	configCursorsStreamPartitions:              {},
	configCursorsStreamReplicationFactor:       {},
	configCursorsStreamAutoPauseTime:           {},
	configGroupsConsumerTimeout:                {},
	configGroupsCoordinatorTimeout:             {},
}

// StreamsConfig contains settings for controlling the message log for streams.
type StreamsConfig struct {
	RetentionMaxBytes             int64
	RetentionMaxMessages          int64
	RetentionMaxAge               time.Duration
	CleanerInterval               time.Duration
	SegmentMaxBytes               int64
	SegmentMaxAge                 time.Duration
	Compact                       bool
	CompactMaxGoroutines          int
	AutoPauseTime                 time.Duration
	AutoPauseDisableIfSubscribers bool
	MinISR                        int
	ConcurrencyControl            bool
	Encryption                    bool
}

// RetentionString returns a human-readable string representation of the
// retention policy.
func (l StreamsConfig) RetentionString() string {
	str := "["
	prefix := ""
	if l.RetentionMaxMessages != 0 {
		str += fmt.Sprintf("Messages: %s", humanize.Comma(l.RetentionMaxMessages))
		prefix = ", "
	}
	if l.RetentionMaxBytes != 0 {
		str += fmt.Sprintf("%sSize: %s", prefix, humanize.IBytes(uint64(l.RetentionMaxBytes)))
		prefix = ", "
	}
	if l.RetentionMaxAge > 0 {
		str += fmt.Sprintf("%sAge: %s", prefix, durafmt.Parse(l.RetentionMaxAge))
		prefix = ", "
	}
	if prefix == "" {
		str += "no limits"
	}
	str += fmt.Sprintf(", Compact: %t", l.Compact)
	str += "]"
	return str
}

// AutoPauseString returns a human-readable string representation of the auto
// pause setting.
func (l StreamsConfig) AutoPauseString() string {
	str := "disabled"
	if l.AutoPauseTime > 0 {
		str = durafmt.Parse(l.AutoPauseTime).String()
	}
	return str
}

// ApplyOverrides applies the values from the StreamConfig protobuf to the
// StreamsConfig struct. If the value is present in the request's config
// section, it will be set in StreamsConfig.
func (l *StreamsConfig) ApplyOverrides(c *proto.StreamConfig) {
	if c == nil {
		return
	}

	// By default, duration configuration is considered as milliseconds.
	if retentionMaxAge := c.RetentionMaxAge; retentionMaxAge != nil {
		l.RetentionMaxAge = time.Duration(retentionMaxAge.Value) * time.Millisecond
	}

	if cleanerInterval := c.CleanerInterval; cleanerInterval != nil {
		l.CleanerInterval = time.Duration(cleanerInterval.Value) * time.Millisecond
	}

	if segmentMaxAge := c.SegmentMaxAge; segmentMaxAge != nil {
		l.SegmentMaxAge = time.Duration(segmentMaxAge.Value) * time.Millisecond
	}

	if maxBytes := c.RetentionMaxBytes; maxBytes != nil {
		l.RetentionMaxBytes = maxBytes.Value
	}

	if maxMessages := c.RetentionMaxMessages; maxMessages != nil {
		l.RetentionMaxMessages = maxMessages.Value
	}

	if segmentMaxBytes := c.SegmentMaxBytes; segmentMaxBytes != nil {
		l.SegmentMaxBytes = segmentMaxBytes.Value
	}

	if compactEnabled := c.CompactEnabled; compactEnabled != nil {
		l.Compact = compactEnabled.Value
	}

	if maxGoroutines := c.CompactMaxGoroutines; maxGoroutines != nil {
		l.CompactMaxGoroutines = int(maxGoroutines.Value)
	}

	if autoPauseTime := c.AutoPauseTime; autoPauseTime != nil {
		l.AutoPauseTime = time.Duration(autoPauseTime.Value) * time.Millisecond
	}

	if autoPauseDisableIfSubscribers := c.AutoPauseDisableIfSubscribers; autoPauseDisableIfSubscribers != nil {
		l.AutoPauseDisableIfSubscribers = autoPauseDisableIfSubscribers.Value
	}

	if minISR := c.MinIsr; minISR != nil {
		l.MinISR = int(minISR.Value)
	}

	if optimisticConcurrencyControl := c.OptimisticConcurrencyControl; optimisticConcurrencyControl != nil {
		l.ConcurrencyControl = optimisticConcurrencyControl.Value
	}

	if encryption := c.Encryption; encryption != nil {
		l.Encryption = encryption.Value
	}
}

// ClusteringConfig contains settings for controlling cluster behavior.
type ClusteringConfig struct {
	ServerID                string
	Namespace               string
	RaftSnapshots           int
	RaftSnapshotThreshold   uint64
	RaftCacheSize           int
	RaftBootstrapSeed       bool
	RaftBootstrapPeers      []string
	RaftMaxQuorumSize       uint
	ReplicaMaxLagTime       time.Duration
	ReplicaMaxLeaderTimeout time.Duration
	ReplicaFetchTimeout     time.Duration
	ReplicaMaxIdleWait      time.Duration
	MinISR                  int
	ReplicationMaxBytes     int64
}

// ActivityStreamConfig contains settings for controlling activity stream
// behavior.
type ActivityStreamConfig struct {
	Enabled          bool
	PublishTimeout   time.Duration
	PublishAckPolicy client.AckPolicy
}

// CursorsStreamConfig contains settings for controlling cursors stream
// behavior.
type CursorsStreamConfig struct {
	Partitions        int32
	ReplicationFactor int32
	AutoPauseTime     time.Duration
}

// GroupsConfig contains settings for controlling consumer group behavior.
type GroupsConfig struct {
	ConsumerTimeout    time.Duration
	CoordinatorTimeout time.Duration
}

// Config contains all settings for a Liftbridge Server.
type Config struct {
	Listen               HostPort
	Host                 string
	Port                 int
	LogLevel             uint32
	LogRecovery          bool
	LogRaft              bool
	LogNATS              bool
	LogSilent            bool
	DataDir              string
	BatchMaxMessages     int
	BatchMaxTime         time.Duration
	MetadataCacheMaxAge  time.Duration
	TLSKey               string
	TLSCert              string
	TLSClientAuth        bool
	TLSClientAuthCA      string
	TLSClientAuthz       bool
	TLSClientAuthzModel  string
	TLSClientAuthzPolicy string
	NATS                 nats.Options
	EmbeddedNATS         bool
	EmbeddedNATSConfig   string
	Streams              StreamsConfig
	Clustering           ClusteringConfig
	ActivityStream       ActivityStreamConfig
	CursorsStream        CursorsStreamConfig
	Groups               GroupsConfig
}

// NewDefaultConfig creates a new Config with default settings.
func NewDefaultConfig() *Config {
	config := &Config{
		NATS: nats.GetDefaultOptions(),
		Port: DefaultPort,
	}
	config.LogLevel = uint32(log.InfoLevel)
	config.BatchMaxMessages = defaultBatchMaxMessages
	config.MetadataCacheMaxAge = defaultMetadataCacheMaxAge
	config.NATS.Servers = []string{nats.DefaultURL}
	config.Clustering.ServerID = nuid.Next()
	config.Clustering.Namespace = DefaultNamespace
	config.Clustering.ReplicaMaxLagTime = defaultReplicaMaxLagTime
	config.Clustering.ReplicaMaxLeaderTimeout = defaultReplicaMaxLeaderTimeout
	config.Clustering.ReplicaMaxIdleWait = defaultReplicaMaxIdleWait
	config.Clustering.ReplicaFetchTimeout = defaultReplicaFetchTimeout
	config.Clustering.RaftSnapshots = defaultRaftSnapshots
	config.Clustering.RaftCacheSize = defaultRaftCacheSize
	config.Clustering.MinISR = defaultMinInsyncReplicas
	config.Clustering.ReplicationMaxBytes = defaultReplicationMaxBytes
	config.Streams.SegmentMaxBytes = defaultMaxSegmentBytes
	config.Streams.SegmentMaxAge = defaultMaxSegmentAge
	config.Streams.RetentionMaxAge = defaultRetentionMaxAge
	config.Streams.CleanerInterval = defaultCleanerInterval
	config.Streams.ConcurrencyControl = defaultConcurrencyControl
	config.Streams.Encryption = defaultEncryption
	config.ActivityStream.PublishTimeout = defaultActivityStreamPublishTimeout
	config.ActivityStream.PublishAckPolicy = defaultActivityStreamPublishAckPolicy
	config.CursorsStream.ReplicationFactor = defaultCursorsStreamReplicationFactor
	config.CursorsStream.AutoPauseTime = defaultCursorsStreamAutoPauseTime
	config.Groups.ConsumerTimeout = defaultGroupsConsumerTimeout
	config.Groups.CoordinatorTimeout = defaultGroupsCoordinatorTimeout
	return config
}

// NATSServersString returns a human-readable string representation of the
// list of NATS servers.
func (c Config) NATSServersString() string {
	return "[" + strings.Join(c.NATS.Servers, ", ") + "]"
}

// GetListenAddress returns the address and port to listen to.
func (c Config) GetListenAddress() HostPort {
	if len(c.Listen.Host) > 0 {
		return c.Listen
	}

	if len(c.Host) > 0 {
		return HostPort{
			Host: c.Host,
			Port: c.Port,
		}
	}

	return HostPort{
		Host: defaultListenAddress,
		Port: c.Port,
	}
}

// GetConnectionAddress returns the host if specified and listen otherwise.
func (c Config) GetConnectionAddress() HostPort {
	if len(c.Host) > 0 {
		return HostPort{
			Host: c.Host,
			Port: c.Port,
		}
	}

	if len(c.Listen.Host) > 0 {
		return c.Listen
	}

	return HostPort{
		Host: defaultConnectionAddress,
		Port: c.Port,
	}
}

// GetLogLevel converts the level string to its corresponding int value. It
// returns an error if the level is invalid.
func GetLogLevel(level string) (uint32, error) {
	var l uint32
	switch strings.ToLower(level) {
	case "debug":
		l = uint32(log.DebugLevel)
	case "info":
		l = uint32(log.InfoLevel)
	case "warn":
		l = uint32(log.WarnLevel)
	case "error":
		l = uint32(log.ErrorLevel)
	default:
		return 0, fmt.Errorf("Invalid %s setting %q", configLoggingLevel, level)
	}
	return l, nil
}

// NewConfig creates a new Config with default settings and applies any
// settings from the given configuration file.
func NewConfig(configFile string) (*Config, error) { // nolint: gocyclo
	var (
		config = NewDefaultConfig()
		v      = viper.New()
	)

	// Return default config if config file is not given.
	if configFile == "" {
		return config, nil
	}

	// Expect a yaml config file.
	v.SetConfigFile(configFile)
	v.SetConfigType("yaml")

	// Allow overriding config with environment variables
	v.SetEnvPrefix("LIFTBRIDGE")
	v.AutomaticEnv()

	// Parse the config file.
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrap(err, "Failed to load configuration file")
	}

	// Validate config settings.
	for _, setting := range v.AllKeys() {
		if _, ok := configKeys[setting]; !ok {
			return nil, fmt.Errorf("Unknown configuration setting %q", setting)
		}
	}

	// Reset SegmentMaxAge since this will get overwritten later.
	config.Streams.SegmentMaxAge = 0

	// Process parsed config file here with v.
	if v.IsSet(configListen) {
		hp, err := parseListen(v)
		if err != nil {
			return nil, err
		}

		config.Listen = *hp
	}

	if v.IsSet(configPort) {
		config.Port = v.GetInt(configPort)
	}

	if v.IsSet(configHost) {
		config.Host = v.GetString(configHost)
	}

	if v.IsSet(configLoggingLevel) {
		level := v.GetString(configLoggingLevel)
		levelInt, err := GetLogLevel(level)
		if err != nil {
			return nil, err
		}

		config.LogLevel = levelInt
	}

	if v.IsSet(configLoggingRecovery) {
		config.LogRecovery = v.GetBool(configLoggingRecovery)
	}

	if v.IsSet(configLoggingRaft) {
		config.LogRaft = v.GetBool(configLoggingRaft)
	}

	if v.IsSet(configLoggingNATS) {
		config.LogNATS = v.GetBool(configLoggingNATS)
	}

	if v.IsSet(configDataDir) {
		config.DataDir = v.GetString(configDataDir)
	}

	if v.IsSet(configBatchMaxMessages) {
		config.BatchMaxMessages = v.GetInt(configBatchMaxMessages)
	}

	if v.IsSet(configBatchMaxTime) {
		config.BatchMaxTime = v.GetDuration(configBatchMaxTime)
	}

	if v.IsSet(configMetadataCacheMaxAge) {
		config.MetadataCacheMaxAge = v.GetDuration(configMetadataCacheMaxAge)
	}

	if v.IsSet(configTLSKey) {
		config.TLSKey = v.GetString(configTLSKey)
	}

	if v.IsSet(configTLSCert) {
		config.TLSCert = v.GetString(configTLSCert)
	}

	if v.IsSet(configTLSClientAuthEnabled) {
		config.TLSClientAuth = v.GetBool(configTLSClientAuthEnabled)
	}

	if v.IsSet(configTLSClientAuthCA) {
		config.TLSClientAuthCA = v.GetString(configTLSClientAuthCA)
	}

	if v.IsSet(configTLSClientAuthzEnabled) {
		config.TLSClientAuthz = v.GetBool(configTLSClientAuthEnabled)
	}

	if v.IsSet(configTLSClientAuthzModel) {
		config.TLSClientAuthzModel = v.GetString(configTLSClientAuthzModel)
	}

	if v.IsSet(configTLSClientAuthzPolicy) {
		config.TLSClientAuthzPolicy = v.GetString(configTLSClientAuthzPolicy)
	}
	if err := parseNATSConfig(config, v); err != nil {
		return nil, err
	}
	if err := parseStreamsConfig(config, v); err != nil {
		return nil, err
	}
	if err := parseClusteringConfig(config, v); err != nil {
		return nil, err
	}
	if err := parseActivityStreamConfig(config, v); err != nil {
		return nil, err
	}
	if err := parseCursorsStreamConfig(config, v); err != nil {
		return nil, err
	}
	if err := parseGroupsConfig(config, v); err != nil {
		return nil, err
	}

	// If SegmentMaxAge is not set, default it to the retention time.
	if config.Streams.SegmentMaxAge == 0 {
		config.Streams.SegmentMaxAge = config.Streams.RetentionMaxAge
	}

	return config, nil
}

// parseNATSConfig parses the `nats` section of a config file and populates the
// given nats.Options.
func parseNATSConfig(config *Config, v *viper.Viper) error {
	if v.IsSet(configNATSEmbeddedConfig) {
		config.EmbeddedNATS = true
		config.EmbeddedNATSConfig = v.GetString(configNATSEmbeddedConfig)
	}

	if v.IsSet(configNATSEmbedded) {
		config.EmbeddedNATS = true
	}

	if v.IsSet(configNATSServers) {
		servers := v.GetStringSlice(configNATSServers)
		config.NATS.Servers = servers
	}

	if v.IsSet(configNATSUser) {
		config.NATS.User = v.GetString(configNATSUser)
	}

	if v.IsSet(configNATSPassword) {
		config.NATS.Password = v.GetString(configNATSPassword)
	}

	// NATS TLS config
	// Both Cert and Key must be presented

	if v.IsSet(configNATSCert) && v.IsSet(configNATSKey) {

		// Load cert and key file
		certFile := v.GetString(configNATSCert)
		keyFile := v.GetString(configNATSKey)

		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return err
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}

		// Load CACert if available
		if v.IsSet(configNATSCA) {
			caFile := v.GetString(configNATSCA)
			// Load CA cert
			caCert, err := ioutil.ReadFile(caFile)

			if err != nil {
				return err
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			tlsConfig.RootCAs = caCertPool
		}
		config.NATS.TLSConfig = tlsConfig
	}

	return nil
}

// parseStreamConfig parses the `streams` section of a config file and
// populates the given Config.
func parseStreamsConfig(config *Config, v *viper.Viper) error {
	if v.IsSet(configStreamsRetentionMaxBytes) {
		config.Streams.RetentionMaxBytes = v.GetInt64(configStreamsRetentionMaxBytes)
	}

	if v.IsSet(configStreamsRetentionMaxMessages) {
		config.Streams.RetentionMaxMessages = v.GetInt64(configStreamsRetentionMaxMessages)
	}

	if v.IsSet(configStreamsRetentionMaxAge) {
		config.Streams.RetentionMaxAge = v.GetDuration(configStreamsRetentionMaxAge)
	}

	if v.IsSet(configStreamsCleanerInterval) {
		config.Streams.CleanerInterval = v.GetDuration(configStreamsCleanerInterval)
	}

	if v.IsSet(configStreamsSegmentMaxBytes) {
		config.Streams.SegmentMaxBytes = v.GetInt64(configStreamsSegmentMaxBytes)
	}

	if v.IsSet(configStreamsSegmentMaxAge) {
		config.Streams.SegmentMaxAge = v.GetDuration(configStreamsSegmentMaxAge)
	}

	if v.IsSet(configStreamsCompactEnabled) {
		config.Streams.Compact = v.GetBool(configStreamsCompactEnabled)
	}

	if v.IsSet(configStreamsCompactMaxGoroutines) {
		config.Streams.CompactMaxGoroutines = v.GetInt(configStreamsCompactMaxGoroutines)
	}

	if v.IsSet(configStreamsAutoPauseTime) {
		config.Streams.AutoPauseTime = v.GetDuration(configStreamsAutoPauseTime)
	}

	if v.IsSet(configStreamsAutoPauseDisableIfSubscribers) {
		config.Streams.AutoPauseDisableIfSubscribers = v.GetBool(configStreamsAutoPauseDisableIfSubscribers)
	}
	if v.IsSet(configStreamsConcurrencyControl) {
		config.Streams.ConcurrencyControl = v.GetBool(configStreamsConcurrencyControl)
	}
	if v.IsSet(configStreamsEncryption) {
		config.Streams.Encryption = v.GetBool(configStreamsEncryption)
	}
	return nil
}

// parseClusteringConfig parses the `clustering` section of a config file and
// populates the given Config.
func parseClusteringConfig(config *Config, v *viper.Viper) error { // nolint: gocyclo
	if v.IsSet(configClusteringServerID) {
		config.Clustering.ServerID = v.GetString(configClusteringServerID)
	}

	if v.IsSet(configClusteringNamespace) {
		config.Clustering.Namespace = v.GetString(configClusteringNamespace)
	}

	if v.IsSet(configClusteringRaftSnapshotRetain) {
		config.Clustering.RaftSnapshots = v.GetInt(configClusteringRaftSnapshotRetain)
	}

	if v.IsSet(configClusteringRaftSnapshotThreshold) {
		config.Clustering.RaftSnapshotThreshold = uint64(v.GetInt64(configClusteringRaftSnapshotThreshold))
	}

	if v.IsSet(configClusteringRaftCacheSize) {
		config.Clustering.RaftCacheSize = v.GetInt(configClusteringRaftCacheSize)
	}

	if v.IsSet(configClusteringRaftBootstrapSeed) {
		config.Clustering.RaftBootstrapSeed = v.GetBool(configClusteringRaftBootstrapSeed)
	}

	if v.IsSet(configClusteringRaftBootstrapPeers) {
		config.Clustering.RaftBootstrapPeers = v.GetStringSlice(configClusteringRaftBootstrapPeers)
	}

	if v.IsSet(configClusteringRaftMaxQuorumSize) {
		config.Clustering.RaftMaxQuorumSize = v.GetUint(configClusteringRaftMaxQuorumSize)
	}

	if v.IsSet(configClusteringReplicaMaxLagTime) {
		config.Clustering.ReplicaMaxLagTime = v.GetDuration(configClusteringReplicaMaxLagTime)
	}

	if v.IsSet(configClusteringReplicaMaxLeaderTimeout) {
		config.Clustering.ReplicaMaxLeaderTimeout = v.GetDuration(configClusteringReplicaMaxLeaderTimeout)
	}

	if v.IsSet(configClusteringReplicaMaxIdleWait) {
		config.Clustering.ReplicaMaxIdleWait = v.GetDuration(configClusteringReplicaMaxIdleWait)
	}

	if v.IsSet(configClusteringReplicaFetchTimeout) {
		config.Clustering.ReplicaFetchTimeout = v.GetDuration(configClusteringReplicaFetchTimeout)
	}

	if v.IsSet(configClusteringMinInsyncReplicas) {
		config.Clustering.MinISR = v.GetInt(configClusteringMinInsyncReplicas)
	}

	if v.IsSet(configClusteringReplicationMaxBytes) {
		config.Clustering.ReplicationMaxBytes = v.GetInt64(configClusteringReplicationMaxBytes)
	}

	return nil
}

// parseActivityStreamConfig parses the `activitystream` section of a config
// file and populates the given Config.
func parseActivityStreamConfig(config *Config, v *viper.Viper) error { // nolint: gocyclo
	if v.IsSet(configActivityStreamEnabled) {
		config.ActivityStream.Enabled = v.GetBool(configActivityStreamEnabled)
	}

	if v.IsSet(configActivityStreamPublishTimeout) {
		config.ActivityStream.PublishTimeout = v.GetDuration(configActivityStreamPublishTimeout)
	}

	if v.IsSet(configActivityStreamPublishAckPolicy) {
		ackPolicy, err := parseAckPolicy(v)
		if err != nil {
			return err
		}

		config.ActivityStream.PublishAckPolicy = ackPolicy
	}

	return nil
}

// parseCursorsStreamConfig parses the `cursors` section of a config file and
// populates the given Config.
func parseCursorsStreamConfig(config *Config, v *viper.Viper) error { // nolint: gocyclo
	if v.IsSet(configCursorsStreamPartitions) {
		config.CursorsStream.Partitions = v.GetInt32(configCursorsStreamPartitions)
	}

	if v.IsSet(configCursorsStreamReplicationFactor) {
		config.CursorsStream.ReplicationFactor = v.GetInt32(configCursorsStreamReplicationFactor)
	}

	if v.IsSet(configCursorsStreamAutoPauseTime) {
		config.CursorsStream.AutoPauseTime = v.GetDuration(configCursorsStreamAutoPauseTime)
	}

	return nil
}

// parseGroupsConfig parses the `groups` section of a config file and populates
// the given Config.
func parseGroupsConfig(config *Config, v *viper.Viper) error { // nolint: gocyclo
	if v.IsSet(configGroupsConsumerTimeout) {
		config.Groups.ConsumerTimeout = v.GetDuration(configGroupsConsumerTimeout)
	}

	if v.IsSet(configGroupsCoordinatorTimeout) {
		config.Groups.CoordinatorTimeout = v.GetDuration(configGroupsCoordinatorTimeout)
	}

	return nil
}

// HostPort is simple struct to hold parsed listen/addr strings.
type HostPort struct {
	Host string
	Port int
}

// parseListen will parse the `listen` option containing the host and port.
func parseListen(v *viper.Viper) (*HostPort, error) {
	hp := &HostPort{}
	listenConf := v.Get(configListen)
	switch listenConf := listenConf.(type) {
	// Only a port
	case int64:
		hp.Port = int(listenConf)
	case string:
		host, port, err := net.SplitHostPort(listenConf)
		if err != nil {
			return nil, fmt.Errorf("Could not parse address string %q", listenConf)
		}
		hp.Port, err = strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("Could not parse port %q", port)
		}
		hp.Host = host
	}
	return hp, nil
}

// parseAckPolicy will parse the activity stream's `ack.policy` option
// containing the ack policy to use when publishing activity events.
func parseAckPolicy(v *viper.Viper) (client.AckPolicy, error) {
	ackPolicy := v.GetString(configActivityStreamPublishAckPolicy)
	switch ackPolicy {
	case "none":
		return client.AckPolicy_NONE, nil
	case "leader":
		return client.AckPolicy_LEADER, nil
	case "all":
		return client.AckPolicy_ALL, nil
	default:
		return defaultActivityStreamPublishAckPolicy, fmt.Errorf("Unknown activity stream publish ack policy %q", ackPolicy)
	}
}
