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

	"github.com/tylertreat/liftbridge/server/conf"
)

const (
	defaultNamespace               = "liftbridge-default"
	defaultReplicaMaxLagTime       = 10 * time.Second
	defaultReplicaMaxLeaderTimeout = 10 * time.Second
	defaultRaftSnapshots           = 2
	defaultRetentionMaxBytes       = -1
	defaultMetadataCacheMaxAge     = 2 * time.Minute
	defaultBatchMaxMessages        = 1024
)

type LogConfig struct {
	RetentionMaxBytes int64
	SegmentMaxBytes   int64
	Compact           bool
}

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
}

type Config struct {
	Host                string
	Port                int
	LogLevel            uint32
	DataPath            string
	BatchMaxMessages    int
	BatchWaitTime       time.Duration
	MetadataCacheMaxAge time.Duration
	NATS                nats.Options
	Log                 LogConfig
	Clustering          ClusteringConfig
}

func NewConfig(configFile string) (*Config, error) {
	config := &Config{
		NATS: nats.GetDefaultOptions(),
	}

	// Defaults
	config.LogLevel = uint32(log.InfoLevel)
	config.BatchMaxMessages = defaultBatchMaxMessages
	config.MetadataCacheMaxAge = defaultMetadataCacheMaxAge
	config.Clustering.ServerID = nuid.Next()
	config.Clustering.Namespace = defaultNamespace
	config.Clustering.ReplicaMaxLagTime = defaultReplicaMaxLagTime
	config.Clustering.ReplicaMaxLeaderTimeout = defaultReplicaMaxLeaderTimeout
	config.Clustering.RaftSnapshots = defaultRaftSnapshots
	config.Log.RetentionMaxBytes = defaultRetentionMaxBytes

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
			switch strings.ToLower(v.(string)) {
			case "debug":
				config.LogLevel = uint32(log.DebugLevel)
			case "info":
				config.LogLevel = uint32(log.InfoLevel)
			case "warn":
				config.LogLevel = uint32(log.WarnLevel)
			case "error":
				config.LogLevel = uint32(log.ErrorLevel)
			default:
				return nil, fmt.Errorf("Invalid log.level setting %q", v.(string))
			}
		case "data.path":
			config.DataPath = v.(string)
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
			if err := parseNATSConfig(v.(map[string]interface{}), config.NATS); err != nil {
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

func parseNATSConfig(m map[string]interface{}, opts nats.Options) error {
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

func parseLogConfig(config *Config, m map[string]interface{}) error {
	// Defaults
	config.Log.RetentionMaxBytes = -1

	for k, v := range m {
		switch strings.ToLower(k) {
		case "retention.max.bytes":
			config.Log.RetentionMaxBytes = v.(int64)
		case "segment.max.bytes":
			config.Log.SegmentMaxBytes = v.(int64)
		case "compact":
			config.Log.Compact = v.(bool)
		default:
			return fmt.Errorf("Unknown log configuration setting %q", k)
		}
	}
	return nil
}

func parseClusteringConfig(config *Config, m map[string]interface{}) error {
	// Defaults
	config.Clustering.RaftSnapshots = 2
	config.Clustering.RaftCacheSize = 512

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
		case "raft.bootstrap":
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

// parseListen will parse listen option which is replacing host/net and port
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
