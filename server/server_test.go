package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/liftbridge-io/liftbridge-grpc/go"
	natsdTest "github.com/nats-io/nats-server/v2/test"
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

func getTestConfig(id string, bootstrap bool, port int) *Config {
	config := NewDefaultConfig()
	config.Clustering.RaftBootstrapSeed = bootstrap
	config.DataDir = filepath.Join(storagePath, id)
	config.Clustering.RaftSnapshots = 1
	config.Clustering.RaftLogging = true
	config.Clustering.ServerID = id
	config.LogLevel = uint32(log.DebugLevel)
	config.NATS.Servers = []string{"nats://localhost:4222"}
	config.LogSilent = true
	config.Port = port
	return config
}

func runServerWithConfig(t *testing.T, config *Config) *Server {
	server, err := RunServerWithConfig(config)
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
			if !s.IsRunning() || s.getRaft() == nil {
				continue
			}
			if s.IsLeader() {
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
	if leader == nil {
		stackFatalf(t, "No metadata leader found")
	}
	return leader
}

func waitForNoMetadataLeader(t *testing.T, timeout time.Duration, servers ...*Server) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader string
		for _, s := range servers {
			if l := string(s.getRaft().Leader()); l != "" {
				leader = l
				break
			}
		}
		if leader == "" {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	stackFatalf(t, "Metadata leader found")
}

func getPartitionLeader(t *testing.T, timeout time.Duration, name string, partitionID int32, servers ...*Server) *Server {
	var (
		leader   *Server
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if !s.IsRunning() {
				continue
			}
			partition := s.metadata.GetPartition(name, partitionID)
			if partition == nil {
				continue
			}
			streamLeader, _ := partition.GetLeader()
			if streamLeader == s.config.Clustering.ServerID {
				if leader != nil {
					stackFatalf(t, "Found more than one stream leader")
				}
				leader = s
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if leader == nil {
		stackFatalf(t, "No stream leader found")
	}
	return leader
}

func forceLogClean(t *testing.T, subject, name string, s *Server) {
	partition := s.metadata.GetPartition(name, 0)
	if partition == nil {
		stackFatalf(t, "Stream not found")
	}
	if err := partition.log.Clean(); err != nil {
		stackFatalf(t, "Log clean failed: %s", err)
	}
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
	s1Config := getTestConfig("a", false, 0)
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
	s1Config := getTestConfig("a", true, 0)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	leader := getMetadataLeader(t, 10*time.Second, s1)

	future := leader.getRaft().GetConfiguration()
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

	future = leader.getRaft().GetConfiguration()
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
	s1Config := getTestConfig("a", true, 0)
	s1Config.Clustering.ServerID = "a"
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	leader := getMetadataLeader(t, 10*time.Second, s1)

	future := leader.getRaft().GetConfiguration()
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

	future = leader.getRaft().GetConfiguration()
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
	s1Config := getTestConfig("a", true, 0)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server which should automatically join the first.
	s2Config := getTestConfig("b", false, 0)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	var (
		servers = []*Server{s1, s2}
		leader  = getMetadataLeader(t, 10*time.Second, servers...)
	)

	// Verify configuration.
	future := leader.getRaft().GetConfiguration()
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
	s1Config := getTestConfig("a", false, 0)
	s1Config.Clustering.ServerID = "a"
	s1Config.Clustering.RaftBootstrapPeers = []string{"b"}
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 0)
	s2Config.Clustering.ServerID = "b"
	s2Config.Clustering.RaftBootstrapPeers = []string{"a"}
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	var (
		servers = []*Server{s1, s2}
		leader  = getMetadataLeader(t, 10*time.Second, servers...)
	)

	// Verify configuration.
	future := leader.getRaft().GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	configServers := future.Configuration().Servers
	if len(configServers) != 2 {
		t.Fatalf("Expected 2 servers, got %d", len(configServers))
	}

	// Ensure new servers can automatically join once the cluster is formed.
	s3Config := getTestConfig("c", false, 0)
	s3 := runServerWithConfig(t, s3Config)
	defer s3.Stop()

	future = leader.getRaft().GetConfiguration()
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

	s1Config := getTestConfig("a", true, 0)
	s1 := New(s1Config)
	s1FatalLogger := &captureFatalLogger{}
	s1.logger = s1FatalLogger
	err := s1.Start()
	require.NoError(t, err)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	// Configure second server on same cluster as a seed too. Servers should
	// stop.
	s2Config := getTestConfig("b", true, 0)
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

// Ensure when the metadata leader fails, a new one is elected.
func TestMetadataLeaderFailover(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 0)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 0)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Configure third server.
	s3Config := getTestConfig("c", false, 0)
	s3 := runServerWithConfig(t, s3Config)
	defer s3.Stop()

	// Wait for metadata leader to be elected.
	servers := []*Server{s1, s2, s3}
	leader := getMetadataLeader(t, 10*time.Second, servers...)
	followers := []*Server{}
	for _, s := range servers {
		if s == leader {
			continue
		}
		followers = append(followers, s)
	}

	// Kill the leader.
	leader.Stop()

	// Wait for new leader to be elected.
	getMetadataLeader(t, 10*time.Second, followers...)
}

// Ensure when the subscribe offset exceeds the HW, the subscription waits for
// new messages.
func TestSubscribeOffsetOverflow(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Publish some messages.
	num := 5
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"))
		require.NoError(t, err)
	}

	// Subscribe with overflowed offset. This should wait for new messages
	// starting at offset 5.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(5), msg.Offset)
		close(gotMsg)
		cancel()
	}, lift.StartAtOffset(100))
	require.NoError(t, err)

	// Publish one more message.
	_, err = client.Publish(context.Background(), subject, []byte("test"))
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case <-gotMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure when the subscribe offset exceeds the HW due to the stream being
// empty, the subscription waits for new messages.
func TestSubscribeOffsetOverflowEmptyStream(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Subscribe with overflowed offset. This should wait for new messages
	// starting at offset 0.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(0), msg.Offset)
		close(gotMsg)
		cancel()
	})
	require.NoError(t, err)

	// Publish message.
	_, err = client.Publish(context.Background(), subject, []byte("test"))
	require.NoError(t, err)

	// Wait to get the message.
	select {
	case <-gotMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure when the subscribe offset is less than the oldest log offset, the
// offset is set to the oldest offset.
func TestSubscribeOffsetUnderflow(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	// Set these to force deletion so we can get an underflow.
	s1Config.Log.SegmentMaxBytes = 1
	s1Config.Log.RetentionMaxBytes = 1
	s1Config.BatchMaxMessages = 1
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Publish some messages.
	num := 2
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"))
		require.NoError(t, err)
	}

	// Force log clean.
	forceLogClean(t, subject, name, s1)

	// Subscribe with underflowed offset. This should set the offset to 1.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(1), msg.Offset)
		close(gotMsg)
		cancel()
	}, lift.StartAtOffset(0))
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case <-gotMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure the stream bytes retention ensures data is deleted when the log
// exceeds the limit.
func TestStreamRetentionBytes(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.Log.SegmentMaxBytes = 1
	s1Config.Log.RetentionMaxBytes = 1000
	s1Config.BatchMaxMessages = 1
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Publish some messages.
	num := 100
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"))
		require.NoError(t, err)
	}

	// Force log clean.
	forceLogClean(t, subject, name, s1)

	// The first message read back should have offset 87.
	msgs := make(chan *proto.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, int64(87), msg.Offset)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure the stream messages retention ensures data is deleted when the log
// exceeds the limit.
func TestStreamRetentionMessages(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.Log.SegmentMaxBytes = 1
	s1Config.Log.RetentionMaxMessages = 5
	s1Config.BatchMaxMessages = 1
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Publish some messages.
	num := 10
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"))
		require.NoError(t, err)
	}

	// Force log clean.
	forceLogClean(t, subject, name, s1)

	// The first message read back should have offset 5.
	msgs := make(chan *proto.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, int64(5), msg.Offset)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure the stream message age retention ensures data is deleted when log
// segments exceed the TTL.
func TestStreamRetentionAge(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.Log.SegmentMaxBytes = 1
	s1Config.Log.RetentionMaxAge = time.Nanosecond
	s1Config.BatchMaxMessages = 1
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Publish some messages.
	num := 100
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"))
		require.NoError(t, err)
	}

	// Force log clean.
	forceLogClean(t, subject, name, s1)

	// We expect all segments but the last to be truncated due to age, so the
	// first message read back should have offset 99.
	msgs := make(chan *proto.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, int64(99), msg.Offset)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure Subscribe returns an error when an invalid StartPosition is used.
func TestSubscribeStartPositionInvalid(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Subscribe with invalid StartPosition.
	err = client.Subscribe(context.Background(), name, nil, lift.StartAt(9999))
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unknown StartPosition")
}

// Ensure when StartPosition_EARLIEST is used with Subscribe, messages are read
// starting at the oldest offset.
func TestSubscribeEarliest(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	// Set these to force deletion.
	s1Config.Log.SegmentMaxBytes = 1
	s1Config.Log.RetentionMaxBytes = 1
	s1Config.BatchMaxMessages = 1
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Publish some messages.
	num := 2
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"))
		require.NoError(t, err)
	}

	// Force log clean.
	forceLogClean(t, subject, name, s1)

	// Subscribe with EARLIEST. This should start reading from offset 1.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe(ctx, name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(1), msg.Offset)
		close(gotMsg)
		cancel()
	}, lift.StartAtEarliestReceived())

	// Wait to get the new message.
	select {
	case <-gotMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure when StartPosition_LATEST is used with Subscribe, messages are read
// starting at the newest offset.
func TestSubscribeLatest(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Publish some messages.
	num := 3
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"))
		require.NoError(t, err)
	}

	// Subscribe with LATEST. This should start reading from offset 2.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe(ctx, name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(2), msg.Offset)
		close(gotMsg)
		cancel()
	}, lift.StartAtLatestReceived())

	// Wait to get the new message.
	select {
	case <-gotMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure when StartPosition_NEW_ONLY is used with Subscribe, the subscription
// waits for new messages.
func TestSubscribeNewOnly(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Publish some messages.
	num := 5
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"))
		require.NoError(t, err)
	}

	// Subscribe with NEW_ONLY. This should wait for new messages starting at
	// offset 5.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(5), msg.Offset)
		close(gotMsg)
		cancel()
	})
	require.NoError(t, err)

	// Publish one more message.
	_, err = client.Publish(context.Background(), subject, []byte("test"))
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case <-gotMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure when StartPosition_TIMESTAMP is used with Subscribe, messages are
// read starting at the given timestamp.
func TestSubscribeStartTime(t *testing.T) {
	defer cleanupStorage(t)
	timestampBefore := timestamp
	mockTimestamp := int64(0)
	timestamp = func() int64 {
		time := mockTimestamp
		mockTimestamp += 10
		return time
	}
	defer func() {
		timestamp = timestampBefore
	}()

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Publish some messages.
	num := 5
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"))
		require.NoError(t, err)
	}

	// Subscribe with TIMESTAMP 25. This should start reading from offset 3.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe(ctx, name, func(msg *proto.Message, err error) {
		select {
		case <-gotMsg:
			return
		default:
		}
		require.NoError(t, err)
		require.Equal(t, int64(3), msg.Offset)
		require.Equal(t, int64(30), msg.Timestamp)
		close(gotMsg)
		cancel()
	}, lift.StartAtTime(time.Unix(0, 25)))

	// Wait to get the new message.
	select {
	case <-gotMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure clients can connect with TLS when enabled.
func TestTLS(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server with TLS.
	s1Config, err := NewConfig("./configs/tls.conf")
	require.NoError(t, err)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	// Connect with TLS.
	client, err := lift.Connect([]string{"localhost:5050"}, lift.TLSCert("./configs/certs/server.crt"))
	require.NoError(t, err)
	defer client.Close()

	// Connecting without a cert should fail.
	_, err = lift.Connect([]string{"localhost:5050"})
	require.Error(t, err)
}

// Ensure that the host address is the same as the listen address when
// specifying only the latter
func TestListen(t *testing.T) {
	config, err := NewConfig("./configs/listen.conf")
	require.NoError(t, err)

	ex := HostPort{
		Host: "192.168.0.1",
		Port: 4222,
	}

	r := config.GetListenAddress()
	if r != ex {
		t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", r, ex)
	}

	r = config.GetConnectionAddress()
	if r != ex {
		t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", r, ex)
	}
}

// Ensure that the listen address is the same as the host address when
// specifying only the latter
func TestHost(t *testing.T) {
	config, err := NewConfig("./configs/host.conf")
	require.NoError(t, err)

	ex := HostPort{
		Host: "192.168.0.1",
		Port: 4222,
	}

	r := config.GetListenAddress()
	if r != ex {
		t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", r, ex)
	}

	r = config.GetConnectionAddress()
	if r != ex {
		t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", r, ex)
	}
}

// Ensure that the listen and connection addresses have the expected values
// when specifying both
func TestListenHost(t *testing.T) {
	config, err := NewConfig("./configs/listen-host.conf")
	require.NoError(t, err)

	ex := HostPort{
		Host: "192.168.0.1",
		Port: 4222,
	}

	r := config.GetListenAddress()
	if r != ex {
		t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", r, ex)
	}

	ex = HostPort{
		Host: "my-host",
		Port: 4333,
	}

	r = config.GetConnectionAddress()
	if r != ex {
		t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", r, ex)
	}
}

// Ensure that the listen and connection addresses have the expected default
// values
func TestDefaultListenHost(t *testing.T) {
	config := NewDefaultConfig()

	ex := HostPort{
		Host: defaultListenAddress,
		Port: DefaultPort,
	}

	r := config.GetListenAddress()
	if r != ex {
		t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", r, ex)
	}

	ex = HostPort{
		Host: defaultConnectionAddress,
		Port: DefaultPort,
	}

	r = config.GetConnectionAddress()
	if r != ex {
		t.Fatalf("Not Equal:\nReceived: '%+v'\nExpected: '%+v'\n", r, ex)
	}
}
