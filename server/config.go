package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/hako/durafmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	// DefaultNamespace is the default cluster namespace to use if one is not
	// specified.
	DefaultNamespace = "liftbridge-default"

	// DefaultPort is the port to bind to if one is not specified.
	DefaultPort = 9292
)

const (
	defaultListenAddress           = "0.0.0.0"
	defaultConnectionAddress       = "localhost"
	defaultReplicaMaxLagTime       = 15 * time.Second
	defaultReplicaMaxLeaderTimeout = 15 * time.Second
	defaultReplicaMaxIdleWait      = 10 * time.Second
	defaultRaftSnapshots           = 2
	defaultRaftCacheSize           = 512
	defaultMetadataCacheMaxAge     = 2 * time.Minute
	defaultBatchMaxMessages        = 1024
	defaultReplicaFetchTimeout     = 3 * time.Second
	defaultMinInsyncReplicas       = 1
	defaultRetentionMaxAge         = 7 * 24 * time.Hour
	defaultCleanerInterval         = 5 * time.Minute
	defaultMaxSegmentBytes         = 1024 * 1024 * 256 // 256MB
	defaultLogRollTime             = defaultRetentionMaxAge
)

// LogConfig contains settings for controlling the message log for a stream.
type LogConfig struct {
	RetentionMaxBytes    int64
	RetentionMaxMessages int64
	RetentionMaxAge      time.Duration
	CleanerInterval      time.Duration
	SegmentMaxBytes      int64
	LogRollTime          time.Duration
	Compact              bool
	CompactMaxGoroutines int
}

// RetentionString returns a human-readable string representation of the
// retention policy.
func (l LogConfig) RetentionString() string {
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

// ClusteringConfig contains settings for controlling cluster behavior.
type ClusteringConfig struct {
	ServerID                string
	Namespace               string
	RaftSnapshots           int
	RaftSnapshotThreshold   uint64
	RaftCacheSize           int
	RaftBootstrapSeed       bool
	RaftBootstrapPeers      []string
	RaftLogging             bool
	ReplicaMaxLagTime       time.Duration
	ReplicaMaxLeaderTimeout time.Duration
	ReplicaFetchTimeout     time.Duration
	ReplicaMaxIdleWait      time.Duration
	MinISR                  int
}

// Config contains all settings for a Liftbridge Server.
type Config struct {
	Listen              HostPort
	Host                string
	Port                int
	LogLevel            uint32
	LogRecovery         bool
	LogSilent           bool
	DataDir             string
	BatchMaxMessages    int
	BatchWaitTime       time.Duration
	MetadataCacheMaxAge time.Duration
	TLSKey              string
	TLSCert             string
	TLSClientAuth       bool
	TLSClientAuthCA     string
	NATS                nats.Options
	Log                 LogConfig
	Clustering          ClusteringConfig
}

// new Viper to parse configuration file
func newViper() *viper.Viper {
	v := viper.New()
	return v
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
	config.Clustering.ServerID = nuid.Next()
	config.Clustering.Namespace = DefaultNamespace
	config.Clustering.ReplicaMaxLagTime = defaultReplicaMaxLagTime
	config.Clustering.ReplicaMaxLeaderTimeout = defaultReplicaMaxLeaderTimeout
	config.Clustering.ReplicaMaxIdleWait = defaultReplicaMaxIdleWait
	config.Clustering.ReplicaFetchTimeout = defaultReplicaFetchTimeout
	config.Clustering.RaftSnapshots = defaultRaftSnapshots
	config.Clustering.RaftCacheSize = defaultRaftCacheSize
	config.Clustering.MinISR = defaultMinInsyncReplicas
	config.Log.SegmentMaxBytes = defaultMaxSegmentBytes
	config.Log.RetentionMaxAge = defaultRetentionMaxAge
	config.Log.LogRollTime = defaultLogRollTime
	config.Log.CleanerInterval = defaultCleanerInterval
	return config
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
		return 0, fmt.Errorf("Invalid log.level setting %q", level)
	}
	return l, nil
}

// NewConfig creates a new Config with default settings and applies any
// settings from the given configuration file.
func NewConfig(configFile string) (*Config, error) { // nolint: gocyclo
	v := newViper()
	// Expect a config.yaml file in the destination
	v.SetConfigFile(configFile)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// return default config
			config := NewDefaultConfig()
			return config, nil
		}
	}

	config := new(Config)
	// Reset LogRollTime since this will get overwritten later.
	config.Log.LogRollTime = 0

	//Parse config file here with v
	if v.IsSet("listen") {
		hp, err := parseListen(v)
		if err != nil {
			return nil, err
		}

		config.Listen = *hp
	}

	config.LogRecovery = v.GetBool("log.recovery")
	if v.IsSet("port") {
		config.Port = v.GetInt("port")
	}

	if v.IsSet("host") {
		config.Host = v.GetString("host")
	}

	if v.IsSet("log.level") {
		level := v.GetString("log.level")
		levelInt, err := GetLogLevel(level)
		if err != nil {
			return nil, err
		}

		config.LogLevel = levelInt
	}

	if v.IsSet("log.recovery") {
		config.LogRecovery = v.GetBool("log.recovery")
	}

	if v.IsSet("data.dir") {
		config.DataDir = v.GetString("data.dir")
	}

	if v.IsSet("batch.max.messages") {
		config.BatchMaxMessages = v.GetInt("batch.max.messages")
	}

	if v.IsSet("batch.wait.time") {
		waitTime := v.GetString("batch.wait.time")
		durWaitTime, err := time.ParseDuration(waitTime)
		if err != nil {
			return nil, err
		}
		config.BatchWaitTime = durWaitTime
	}

	if v.IsSet("metadata.cache.max.age") {
		maxAge := v.GetString("metadata.cache.max.age")
		durMaxAge, err := time.ParseDuration(maxAge)
		if err != nil {
			return nil, err
		}
		config.MetadataCacheMaxAge = durMaxAge
	}

	if v.IsSet("tls.key") {
		config.TLSKey = v.GetString("tls.key")
	}

	if v.IsSet("tls.cert") {
		config.TLSCert = v.GetString("tls.cert")
	}

	if v.IsSet("tls.client.auth") {
		config.TLSClientAuth = v.GetBool("tls.client.auth")
	}

	if v.IsSet("tls.client.auth.ca") {
		config.TLSClientAuthCA = v.GetString("tls.client.auth.ca")
	}

	if err := parseNATSConfig(&config.NATS, v); err != nil {
		return nil, err
	}

	if err := parseLogConfig(config, v); err != nil {
		return nil, err
	}

	if err := parseClusteringConfig(config, v); err != nil {
		return nil, err
	}
	// If LogRollTime is not set, default it to the retention time.
	if config.Log.LogRollTime == 0 {
		config.Log.LogRollTime = config.Log.RetentionMaxAge
	}

	return config, nil
}

// parseNATSConfig parses the `nats` section of a config file and populates the
// given nats.Options.
func parseNATSConfig(opts *nats.Options, v *viper.Viper) error {
	if v.IsSet("nats.servers") {
		servers := v.GetStringSlice("nats.servers")
		opts.Servers = servers
	}

	if v.IsSet("nats.user") {
		opts.User = v.GetString("nats.user")
	}

	if v.IsSet("nats.password") {
		opts.Password = v.GetString("nats.password")
	}

	return nil
}

// parseLogConfig parses the `log` section of a config file and populates the
// given Config.
func parseLogConfig(config *Config, v *viper.Viper) error {

	if v.IsSet("log.retention.max.bytes") {
		config.Log.RetentionMaxBytes = v.GetInt64("log.retention.max.bytes")
	}

	if v.IsSet("log.retention.max.messages") {
		config.Log.RetentionMaxMessages = v.GetInt64("log.retention.max.messages")
	}

	if v.IsSet("log.retention.max.age") {
		durMaxAgeStr := v.GetString("log.retention.max.age")
		durMaxAge, err := time.ParseDuration(durMaxAgeStr)
		if err != nil {
			return err
		}
		config.Log.RetentionMaxAge = durMaxAge
	}

	if v.IsSet("log.cleaner.interval") {
		cleanerIntervalStr := v.GetString("log.cleaner.interval")
		durCleanerInterval, err := time.ParseDuration(cleanerIntervalStr)
		if err != nil {
			return err
		}
		config.Log.CleanerInterval = durCleanerInterval
	}

	if v.IsSet("log.segment.max.bytes") {
		config.Log.SegmentMaxBytes = v.GetInt64("log.segment.max.bytes")
	}

	if v.IsSet("log.roll.time") {
		rollTimeStr := v.GetString("log.roll.time")
		durRollTime, err := time.ParseDuration(rollTimeStr)
		if err != nil {
			return err
		}
		config.Log.LogRollTime = durRollTime
	}

	if v.IsSet("log.compact.compact") {
		config.Log.Compact = v.GetBool("log.compact.compact")
	}

	if v.IsSet("log.compact.max.goroutines") {
		config.Log.CompactMaxGoroutines = v.GetInt("log.compact.max.goroutines")
	}

	return nil
}

// parseClusteringConfig parses the `clustering` section of a config file and
// populates the given Config.
func parseClusteringConfig(config *Config, v *viper.Viper) error { // nolint: gocyclo

	if v.IsSet("clustering.server.id") {
		config.Clustering.ServerID = v.GetString("clustering.server.id")
	}
	if v.IsSet("clustering.namespace") {
		config.Clustering.Namespace = v.GetString("clustering.namespace")
	}
	if v.IsSet("clustering.raft.snapshot.retain") {
		config.Clustering.RaftSnapshots = v.GetInt("clustering.raft.snapshot.retain")
	}
	if v.IsSet("clustering.raft.snapshot.threshold") {
		config.Clustering.RaftSnapshotThreshold = uint64(v.GetInt64("clustering.raft.snapshot.threshold"))
	}
	if v.IsSet("clustering.raft.cache.size") {
		config.Clustering.RaftCacheSize = v.GetInt("clustering.raft.cache.size")
	}
	if v.IsSet("clustering.raft.bootstrap.seed") {
		config.Clustering.RaftBootstrapSeed = v.GetBool("clustering.raft.bootstrap.seed")
	}
	if v.IsSet("clustering.raft.bootstrap.peers") {
		config.Clustering.RaftBootstrapPeers = v.GetStringSlice("clustering.raft.bootstrap.peers")
	}

	if v.IsSet("clustering.raft.logging") {
		config.Clustering.RaftLogging = v.GetBool("clustering.raft.logging")
	}
	if v.IsSet("clustering.replica.max.lag.time") {
		dur, err := time.ParseDuration(v.GetString("clustering.replica.max.lag.time"))
		if err != nil {
			return err
		}
		config.Clustering.ReplicaMaxLagTime = dur
	}

	if v.IsSet("clustering.replica.max.leader.timeout") {
		dur, err := time.ParseDuration(v.GetString("clustering.replica.max.leader.timeout"))
		if err != nil {
			return err
		}
		config.Clustering.ReplicaMaxLeaderTimeout = dur
	}

	if v.IsSet("clustering.replica.max.idle.wait") {
		dur, err := time.ParseDuration(v.GetString("clustering.replica.max.idle.wait"))
		if err != nil {
			return err
		}
		config.Clustering.ReplicaMaxIdleWait = dur
	}

	if v.IsSet("clustering.replica.fetch.timeout") {
		dur, err := time.ParseDuration(v.GetString("clustering.replica.fetch.timeout"))
		if err != nil {
			return err
		}
		config.Clustering.ReplicaFetchTimeout = dur
	}

	if v.IsSet("clustering.min.insync.replicas") {
		config.Clustering.MinISR = v.GetInt("clustering.min.insync.replicas")
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
	listenConf := v.Get("listen")
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
