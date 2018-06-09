package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	natsdTest "github.com/nats-io/gnatsd/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var storagePath string

func init() {
	tmpDir, err := ioutil.TempDir("", "liftbridge_test_")
	if err != nil {
		panic(fmt.Errorf("Error creating temp dir: %v", err))
	}
	if err := os.Remove(tmpDir); err != nil {
		panic(fmt.Errorf("Error removing temp dir: %v", err))
	}
	storagePath = tmpDir
}

func cleanupStorage(t *testing.T) {
	err := os.RemoveAll(storagePath)
	require.NoError(t, err)
}

func getTestConfig(id string, bootstrap bool) *Config {
	config := NewDefaultConfig()
	config.Clustering.RaftBootstrap = bootstrap
	config.DataPath = filepath.Join(storagePath, id)
	config.Clustering.RaftSnapshots = 1
	config.Clustering.RaftLogging = true
	config.LogLevel = uint32(log.DebugLevel)
	config.NATS.Servers = []string{"nats://localhost:4222"}
	config.NoLog = true
	return config
}

func runServerWithConfig(t *testing.T, config *Config) *Server {
	server := New(config)
	err := server.Start()
	require.NoError(t, err)
	return server
}

func getMetadataLeader(t *testing.T, timeout time.Duration, servers ...*Server) *Server {
	var (
		leader   *Server
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if !s.isRunning() || s.raft == nil {
				continue
			}
			if s.isLeader() {
				if leader != nil {
					stackFatalf(t, "Found more than one metadata leader")
				}
				leader = s
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	return leader
}

// Ensure starting a node fails when there is no seed node to join and no
// cluster topology is provided.
func TestNoSeed(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server. Starting this should fail because there is no
	// seed node.
	raftJoinAttempts = 1
	defer func() {
		raftJoinAttempts = defaultRaftJoinAttempts
	}()
	s1Config := getTestConfig("a", false)
	server := New(s1Config)
	err := server.Start()
	require.Error(t, err)
}

// Ensure server ID is assigned when not provided and stored/recovered on
// server restart.
func TestAssignedDurableServerID(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	leader := getMetadataLeader(t, 10*time.Second, s1)

	future := leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	id := future.Configuration().Servers[0].ID

	if id == "" {
		t.Fatal("Expected non-empty cluster server id")
	}

	// Restart server without setting ID.
	s1.Stop()
	s1Config.Clustering.ServerID = ""
	s1 = runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	leader = getMetadataLeader(t, 10*time.Second, s1)

	future = leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	newID := future.Configuration().Servers[0].ID
	if id != newID {
		t.Fatalf("Incorrect cluster server id, expected: %s, got: %s", id, newID)
	}
}

// Ensure server ID is stored and recovered on server restart.
func TestDurableServerID(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true)
	s1Config.Clustering.ServerID = "a"
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	leader := getMetadataLeader(t, 10*time.Second, s1)

	future := leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	id := future.Configuration().Servers[0].ID

	if id != "a" {
		t.Fatalf("Incorrect cluster server id, expected: a, got: %s", id)
	}

	// Restart server without setting ID.
	s1.Stop()
	s1Config.Clustering.ServerID = ""
	s1 = runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	leader = getMetadataLeader(t, 10*time.Second, s1)

	future = leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	newID := future.Configuration().Servers[0].ID
	if newID != "a" {
		t.Fatalf("Incorrect cluster server id, expected: a, got: %s", newID)
	}
}

// Ensure starting a cluster with auto configuration works when we start one
// node in bootstrap mode.
func TestBootstrapAutoConfig(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure the first server as a seed.
	s1Config := getTestConfig("a", true)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server which should automatically join the first.
	s2Config := getTestConfig("b", false)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	var (
		servers = []*Server{s1, s2}
		leader  = getMetadataLeader(t, 10*time.Second, servers...)
	)

	// Verify configuration.
	future := leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	configServers := future.Configuration().Servers
	if len(configServers) != 2 {
		t.Fatalf("Expected 2 servers, got %d", len(configServers))
	}
}

// Ensure starting a cluster with manual configuration works when we provide
// the cluster configuration to each server.
func TestBootstrapManualConfig(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", false)
	s1Config.Clustering.ServerID = "a"
	s1Config.Clustering.RaftBootstrapPeers = []string{"b"}
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false)
	s2Config.Clustering.ServerID = "b"
	s2Config.Clustering.RaftBootstrapPeers = []string{"a"}
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	var (
		servers = []*Server{s1, s2}
		leader  = getMetadataLeader(t, 10*time.Second, servers...)
	)

	// Verify configuration.
	future := leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	configServers := future.Configuration().Servers
	if len(configServers) != 2 {
		t.Fatalf("Expected 2 servers, got %d", len(configServers))
	}

	// Ensure new servers can automatically join once the cluster is formed.
	s3Config := getTestConfig("c", false)
	s3 := runServerWithConfig(t, s3Config)
	defer s3.Stop()

	future = leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	configServers = future.Configuration().Servers
	if len(configServers) != 3 {
		t.Fatalf("Expected 3 servers, got %d", len(configServers))
	}
}

// Ensure if more than one server is started in bootstrap mode, the servers
// eventually panic.
func TestBootstrapMisconfiguration(t *testing.T) {
	defer cleanupStorage(t)

	bootstrapMisconfigInterval = 100 * time.Millisecond
	defer func() {
		bootstrapMisconfigInterval = defaultBootstrapMisconfigInterval
	}()

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	s1Config := getTestConfig("a", true)
	s1 := New(s1Config)
	s1FatalLogger := &captureFatalLogger{}
	s1.logger = s1FatalLogger
	err := s1.Start()
	require.NoError(t, err)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	// Configure second server on same cluster as a seed too. Servers should
	// stop.
	s2Config := getTestConfig("b", true)
	s2 := New(s2Config)
	s2FatalLogger := &captureFatalLogger{}
	s2.logger = s2FatalLogger
	err = s2.Start()
	require.NoError(t, err)
	defer s2.Stop()

	// After a little while, servers should detect that they were both started
	// with the bootstrap flag and exit.
	ok := false
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		check := func(otherServer *Server, l *captureFatalLogger) bool {
			l.Lock()
			defer l.Unlock()
			if l.fatal != "" {
				if strings.Contains(l.fatal, otherServer.config.Clustering.ServerID) {
					return true
				}
			}
			return false
		}
		ok = check(s1, s2FatalLogger)
		if ok {
			ok = check(s2, s1FatalLogger)
		}
		if ok {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !ok {
		t.Fatal("Servers should have reported fatal error")
	}
}
