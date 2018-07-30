package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	log "github.com/sirupsen/logrus"

	"github.com/liftbridge-io/liftbridge/server/conf"
)

const (
	// DefaultNamespace is the default cluster namespace to use if one is not
	// specified.
	DefaultNamespace = "liftbridge-default"

	// DefaultPort is the port to bind to if one is not specified.
	DefaultPort = 9292
)

const (
	defaultReplicaMaxLagTime       = 10 * time.Second
	defaultReplicaMaxLeaderTimeout = 10 * time.Second
	defaultRaftSnapshots           = 2
	defaultRaftCacheSize           = 512
	defaultMetadataCacheMaxAge     = 2 * time.Minute
	defaultBatchMaxMessages        = 1024
	defaultReplicaFetchTimeout     = 5 * time.Second
)

// LogConfig contains settings for controlling the message log for a stream.
type LogConfig struct {
	RetentionMaxBytes    int64
	RetentionMaxMessages int64
	SegmentMaxBytes      int64
}

// ClusteringConfig contains settings for controlling cluster behavior.
type ClusteringConfig struct {
	ServerID                string
	Namespace               string
	RaftSnapshots           int
	RaftSnapshotThreshold   uint64
	RaftCacheSize           int
	RaftBootstrap           bool
	RaftBootstrapPeers      []string
	RaftLogging             bool
	ReplicaMaxLagTime       time.Duration
	ReplicaMaxLeaderTimeout time.Duration
	ReplicaFetchTimeout     time.Duration
}

// Config contains all settings for a Liftbridge Server.
type Config struct {
	Host                string
	Port                int
	LogLevel            uint32
	NoLog               bool
	DataDir             string
	BatchMaxMessages    int
	BatchWaitTime       time.Duration
	MetadataCacheMaxAge time.Duration
	NATS                nats.Options
	Log                 LogConfig
	Clustering          ClusteringConfig
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
	config.Clustering.ReplicaFetchTimeout = defaultReplicaFetchTimeout
	config.Clustering.RaftSnapshots = defaultRaftSnapshots
	config.Clustering.RaftCacheSize = defaultRaftCacheSize
	return config
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
func NewConfig(configFile string) (*Config, error) {
	config := NewDefaultConfig()

	if configFile == "" {
		return config, nil
	}
	c, err := conf.ParseFile(configFile)
	if err != nil {
		return nil, err
	}

	for k, v := range c {
		switch strings.ToLower(k) {
		case "listen":
			hp, err := parseListen(v)
			if err != nil {
				return nil, err
			}
			config.Host = hp.host
			config.Port = hp.port
		case "port":
			config.Port = int(v.(int64))
		case "host":
			config.Host = v.(string)
		case "log.level":
			level, err := GetLogLevel(v.(string))
			if err != nil {
				return nil, err
			}
			config.LogLevel = level
		case "data.dir":
			config.DataDir = v.(string)
		case "batch.max.messages":
			config.BatchMaxMessages = int(v.(int64))
		case "batch.wait.time":
			dur, err := time.ParseDuration(v.(string))
			if err != nil {
				return nil, err
			}
			config.BatchWaitTime = dur
		case "metadata.cache.max.age":
			dur, err := time.ParseDuration(v.(string))
			if err != nil {
				return nil, err
			}
			config.MetadataCacheMaxAge = dur
		case "nats":
			if err := parseNATSConfig(v.(map[string]interface{}), &config.NATS); err != nil {
				return nil, err
			}
		case "log":
			if err := parseLogConfig(config, v.(map[string]interface{})); err != nil {
				return nil, err
			}
		case "clustering":
			if err := parseClusteringConfig(config, v.(map[string]interface{})); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("Unknown configuration setting %q", k)
		}
	}
	return config, nil
}

// parseNATSConfig parses the `nats` section of a config file and populates the
// given nats.Options.
func parseNATSConfig(m map[string]interface{}, opts *nats.Options) error {
	for k, v := range m {
		switch strings.ToLower(k) {
		case "servers":
			servers := v.([]interface{})
			opts.Servers = make([]string, len(servers))
			for i, p := range servers {
				opts.Servers[i] = p.(string)
			}
		default:
			return fmt.Errorf("Unknown nats configuration setting %q", k)
		}
	}
	return nil
}

// parseLogConfig parses the `log` section of a config file and populates the
// given Config.
func parseLogConfig(config *Config, m map[string]interface{}) error {
	for k, v := range m {
		switch strings.ToLower(k) {
		case "retention.max.bytes":
			config.Log.RetentionMaxBytes = v.(int64)
		case "retention.max.messages":
			config.Log.RetentionMaxMessages = v.(int64)
		case "segment.max.bytes":
			config.Log.SegmentMaxBytes = v.(int64)
		default:
			return fmt.Errorf("Unknown log configuration setting %q", k)
		}
	}
	return nil
}

// parseClusteringConfig parses the `clustering` section of a config file and
// populates the given Config.
func parseClusteringConfig(config *Config, m map[string]interface{}) error {
	for k, v := range m {
		switch strings.ToLower(k) {
		case "server.id":
			config.Clustering.ServerID = v.(string)
		case "namespace":
			config.Clustering.Namespace = v.(string)
		case "raft.snapshot.retain":
			config.Clustering.RaftSnapshots = int(v.(int64))
		case "raft.snapshot.threshold":
			config.Clustering.RaftSnapshotThreshold = uint64(v.(int64))
		case "raft.cache.size":
			config.Clustering.RaftCacheSize = int(v.(int64))
		case "raft.bootstrap.seed":
			config.Clustering.RaftBootstrap = v.(bool)
		case "raft.bootstrap.peers":
			peers := v.([]interface{})
			config.Clustering.RaftBootstrapPeers = make([]string, len(peers))
			for i, p := range peers {
				config.Clustering.RaftBootstrapPeers[i] = p.(string)
			}
		case "raft.logging":
			config.Clustering.RaftLogging = v.(bool)
		case "replica.max.lag.time":
			dur, err := time.ParseDuration(v.(string))
			if err != nil {
				return err
			}
			config.Clustering.ReplicaMaxLagTime = dur
		case "replica.fetch.timeout":
			dur, err := time.ParseDuration(v.(string))
			if err != nil {
				return err
			}
			config.Clustering.ReplicaFetchTimeout = dur
		default:
			return fmt.Errorf("Unknown clustering configuration setting %q", k)
		}
	}
	return nil
}

// hostPort is simple struct to hold parsed listen/addr strings.
type hostPort struct {
	host string
	port int
}

// parseListen will parse the `listen` option containing the host and port.
func parseListen(v interface{}) (*hostPort, error) {
	hp := &hostPort{}
	switch v.(type) {
	// Only a port
	case int64:
		hp.port = int(v.(int64))
	case string:
		host, port, err := net.SplitHostPort(v.(string))
		if err != nil {
			return nil, fmt.Errorf("Could not parse address string %q", v)
		}
		hp.port, err = strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("Could not parse port %q", port)
		}
		hp.host = host
	}
	return hp, nil
}
