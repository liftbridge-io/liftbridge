package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
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
	config.LogRaft = true
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

	// Wait for cluster to agree on leader.
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if string(s.getRaft().Leader()) != leader.config.Clustering.ServerID {
				continue LOOP
			}
		}
		break
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

func checkPartitionPaused(t *testing.T, timeout time.Duration, stream string,
	partitionID int32, paused bool, server *Server) {

	partition := server.metadata.GetPartition(stream, partitionID)
	if partition == nil {
		stackFatalf(t, "Partition not found")
	}
	var (
		deadline = time.Now().Add(timeout)
		isPaused bool
	)
	for time.Now().Before(deadline) {
		isPaused = partition.IsPaused()
		if isPaused == paused {
			return
		}
	}
	stackFatalf(t, "Expected partition paused %v, got %v", paused, isPaused)
}

func checkPartitionReadonly(t *testing.T, timeout time.Duration, stream string,
	partitionID int32, readonly bool, server *Server) {

	partition := server.metadata.GetPartition(stream, partitionID)
	if partition == nil {
		stackFatalf(t, "Partition not found")
	}
	var (
		deadline   = time.Now().Add(timeout)
		isReadonly bool
	)
	for time.Now().Before(deadline) {
		isReadonly = partition.IsReadonly()
		if isReadonly == readonly {
			return
		}
	}
	stackFatalf(t, "Expected partition readonly %v, got %v", readonly, isReadonly)
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

// Ensure server starts the gRPC health service correctly.
func TestHealthServerStartedCorrectly(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure the first server as a seed.
	s1Config := getTestConfig("a", true, 20000)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server which should automatically join the first.
	s2Config := getTestConfig("b", false, 20001)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	conn, err := grpc.Dial("127.0.0.1:20000", []grpc.DialOption{grpc.WithInsecure()}...)
	require.NoError(t, err)
	healthClient := grpc_health_v1.NewHealthClient(conn)
	healthCheckReply, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{Service: "proto.API"})
	require.NoError(t, err)
	if healthCheckReply.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("API service is not healthy when it should be: %v", err)
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Publish(ctx, name, []byte("hello"))
		require.NoError(t, err)
	}

	// Subscribe with overflowed offset. This should wait for new messages
	// starting at offset 5.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(5), msg.Offset())
		close(gotMsg)
		cancel()
	}, lift.StartAtOffset(100))
	require.NoError(t, err)

	// Publish one more message.
	_, err = client.Publish(context.Background(), name, []byte("test"))
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
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(0), msg.Offset())
		close(gotMsg)
		cancel()
	})
	require.NoError(t, err)

	// Publish message.
	_, err = client.Publish(context.Background(), name, []byte("test"))
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
	s1Config.Streams.SegmentMaxBytes = 1
	s1Config.Streams.RetentionMaxBytes = 1
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Publish(ctx, name, []byte("hello"))
		require.NoError(t, err)
	}

	// Force log clean.
	forceLogClean(t, subject, name, s1)

	// Subscribe with underflowed offset. This should set the offset to 1.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(1), msg.Offset())
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
	s1Config.Streams.SegmentMaxBytes = 1
	s1Config.Streams.RetentionMaxBytes = 100
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Publish(ctx, name, []byte("hello"))
		require.NoError(t, err)
	}

	// Force log clean.
	forceLogClean(t, subject, name, s1)

	// The first message read back should have offset 9.
	msgs := make(chan *lift.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, int64(9), msg.Offset())
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
	s1Config.Streams.SegmentMaxBytes = 1
	s1Config.Streams.RetentionMaxMessages = 5
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Publish(ctx, name, []byte("hello"))
		require.NoError(t, err)
	}

	// Force log clean.
	forceLogClean(t, subject, name, s1)

	// The first message read back should have offset 5.
	msgs := make(chan *lift.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, int64(5), msg.Offset())
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
	s1Config.Streams.SegmentMaxBytes = 1
	s1Config.Streams.RetentionMaxAge = time.Nanosecond
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Publish(ctx, name, []byte("hello"))
		require.NoError(t, err)
	}

	// Force log clean.
	forceLogClean(t, subject, name, s1)

	// We expect all segments but the last to be truncated due to age, so the
	// first message read back should have offset 9.
	msgs := make(chan *lift.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, int64(9), msg.Offset())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
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
	s1Config.Streams.SegmentMaxBytes = 1
	s1Config.Streams.RetentionMaxBytes = 1
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Publish(ctx, name, []byte("hello"))
		require.NoError(t, err)
	}

	// Force log clean.
	forceLogClean(t, subject, name, s1)

	// Subscribe with EARLIEST. This should start reading from offset 1.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(1), msg.Offset())
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Publish(ctx, name, []byte("hello"))
		require.NoError(t, err)
	}

	// Subscribe with LATEST. This should start reading from offset 2.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(2), msg.Offset())
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Publish(ctx, name, []byte("hello"))
		require.NoError(t, err)
	}

	// Subscribe with NEW_ONLY. This should wait for new messages starting at
	// offset 5.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(5), msg.Offset())
		close(gotMsg)
		cancel()
	})
	require.NoError(t, err)

	// Publish one more message.
	_, err = client.Publish(context.Background(), name, []byte("test"))
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
	timestampCalls := 0
	timestamp = func() int64 {
		time := mockTimestamp
		// Only increment on every other call because we don't want sendAck to
		// advance the time as it throws off the subscribe test.
		if timestampCalls%2 == 0 {
			mockTimestamp += 10
		}
		timestampCalls++
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.Publish(ctx, name, []byte("hello"))
		require.NoError(t, err)
	}

	// Subscribe with TIMESTAMP 25. This should start reading from offset 3.
	msgCh := make(chan *lift.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		select {
		case <-msgCh:
			return
		default:
		}
		require.NoError(t, err)
		msgCh <- msg
		close(msgCh)
		cancel()
	}, lift.StartAtTime(time.Unix(0, 25)))

	// Wait to get the new message.
	select {
	case msg := <-msgCh:
		require.Equal(t, int64(3), msg.Offset())
		require.Equal(t, int64(30), msg.Timestamp().UnixNano())
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
	s1Config, err := NewConfig("./configs/tls.yaml")
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
	config, err := NewConfig("./configs/listen.yaml")
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
	config, err := NewConfig("./configs/host.yaml")
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
	config, err := NewConfig("./configs/listen-host.yaml")
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

// Ensure the leader flag is set when the server is elected metadata leader and
// unset when it loses leadership.
func TestMetadataLeadershipLifecycle(t *testing.T) {
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

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	// Check leader flag is set.
	require.Equal(t, int64(1), atomic.LoadInt64(&(s1.getRaft().leader)))

	// Kill the follower.
	s2.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leader := atomic.LoadInt64(&(s1.getRaft().leader))
		if leader == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
		continue
	}
	t.Fatal("Expected leader flag to be 0")
}

// Ensure propagation handlers for shrinking and expanding the ISR work
// correctly.
func TestPropagatedShrinkExpandISR(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 0)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Wait for server to elect itself leader.
	controller := getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name,
		lift.ReplicationFactor(2))
	require.NoError(t, err)

	waitForISR(t, 10*time.Second, name, 0, 2, s1, s2)

	leader := getPartitionLeader(t, 10*time.Second, name, 0, s1, s2)
	followerID := s1.config.Clustering.ServerID
	if leader == s1 {
		followerID = s2.config.Clustering.ServerID
	}
	partition := leader.metadata.GetPartition(name, 0)
	require.NotNil(t, partition)
	leaderEpoch := partition.LeaderEpoch

	// Shrink ISR.
	controller.handleShrinkISR(&proto.PropagatedRequest{
		ShrinkISROp: &proto.ShrinkISROp{
			Stream:          name,
			Partition:       0,
			ReplicaToRemove: followerID,
			Leader:          leader.config.Clustering.ServerID,
			LeaderEpoch:     leaderEpoch,
		},
	})

	// Wait for ISR to shrink.
	waitForISR(t, 10*time.Second, name, 0, 1, s1, s2)

	// Expand ISR.
	controller.handleExpandISR(&proto.PropagatedRequest{
		Op: proto.Op_EXPAND_ISR,
		ExpandISROp: &proto.ExpandISROp{
			Stream:       name,
			Partition:    0,
			ReplicaToAdd: followerID,
			Leader:       leader.config.Clustering.ServerID,
			LeaderEpoch:  leaderEpoch,
		},
	})

	// Wait for ISR to expand.
	waitForISR(t, 10*time.Second, name, 0, 2, s1, s2)
}

// Test stream pausing and resuming. A paused stream should re-activate itself
// when a message is published using the Liftbridge API. This test pauses all
// partitions and checks that only the partition that is published to gets
// resumed.
func TestPauseStreamAllPartitions(t *testing.T) {
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
	err = client.CreateStream(context.Background(), subject, name, lift.Partitions(2))
	require.NoError(t, err)

	// Try to pause a non-existing stream.
	err = client.PauseStream(context.Background(), "bar")
	require.Error(t, err)

	// Pause all partitions.
	err = client.PauseStream(context.Background(), name)
	require.NoError(t, err)

	// Make sure pause is idempotent.
	err = client.PauseStream(context.Background(), name)
	require.NoError(t, err)

	// Check that both partitions are paused.
	checkPartitionPaused(t, 5*time.Second, name, 0, true, s1)
	checkPartitionPaused(t, 5*time.Second, name, 1, true, s1)

	// Publish a message to partition 0.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Publish(ctx, name, []byte("hello"))
	require.NoError(t, err)

	msgs := make(chan *lift.Message, 1)
	ctx, cancel = context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, []byte("hello"), msg.Value())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	// Check that partition 0 was resumed but partition 1 is still paused.
	checkPartitionPaused(t, 5*time.Second, name, 0, false, s1)
	checkPartitionPaused(t, 5*time.Second, name, 1, true, s1)
}

// Test stream pausing and resuming. A paused stream should re-activate itself
// when a message is published using the Liftbridge API. This test pauses some
// partitions only.
func TestPauseStreamSomePartitions(t *testing.T) {
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
	err = client.CreateStream(context.Background(), subject, name, lift.Partitions(2))
	require.NoError(t, err)

	// Try to pause a non-existing partition.
	err = client.PauseStream(context.Background(), name, lift.PausePartitions(99))
	require.Error(t, err)

	// Pause partition 0 but set ResumeAll.
	err = client.PauseStream(context.Background(), name, lift.PausePartitions(0), lift.ResumeAll())
	require.NoError(t, err)

	// Check that only partition 0 is paused.
	checkPartitionPaused(t, 5*time.Second, name, 0, true, s1)
	checkPartitionPaused(t, 5*time.Second, name, 1, false, s1)

	// Publish a message to the non-paused partition, which should resume the
	// paused partition since ResumeAll was enabled.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Publish(ctx, name, []byte("hello"), lift.ToPartition(1))
	require.NoError(t, err)

	msgs := make(chan *lift.Message, 1)
	ctx, cancel = context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived(), lift.Partition(1))
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, []byte("hello"), msg.Value())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	// Check that both partitions are resumed.
	checkPartitionPaused(t, 5*time.Second, name, 0, false, s1)
	checkPartitionPaused(t, 5*time.Second, name, 1, false, s1)
}

// Ensure pausing a stream works when we send the request to the metadata
// follower and resuming the stream works when the resuming publish is sent to
// the follower.
func TestPauseStreamPropagate(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 0)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5050)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Connect and send the request to the follower.
	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	name := "foo"
	err = client.CreateStream(ctx, "foo", name)
	require.NoError(t, err)

	// Pause stream on follower.
	err = client.PauseStream(context.Background(), name)
	require.NoError(t, err)

	checkPartitionPaused(t, 5*time.Second, name, 0, true, s1)
	checkPartitionPaused(t, 5*time.Second, name, 0, true, s2)

	// Resume stream by publishing to follower.
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Publish(ctx, name, []byte("hello"))
	require.NoError(t, err)

	checkPartitionPaused(t, 5*time.Second, name, 0, false, s1)
	checkPartitionPaused(t, 5*time.Second, name, 0, false, s2)
}

// Test setting and removing the readonly flag on a stream. This test sets all
// partitions on a stream as readonly and checks that both cannot be published
// to.
func TestSetStreamReadonlyAllPartitions(t *testing.T) {
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
	err = client.CreateStream(context.Background(), subject, name, lift.Partitions(2))
	require.NoError(t, err)

	// Set the stream as readonly.
	err = client.SetStreamReadonly(context.Background(), name)
	require.NoError(t, err)

	// Check that both partitions are readonly.
	checkPartitionReadonly(t, 5*time.Second, name, 0, true, s1)
	checkPartitionReadonly(t, 5*time.Second, name, 1, true, s1)

	// Check that both partitions cannot be published to.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Publish(ctx, name, []byte("hello"), lift.ToPartition(0))
	require.Error(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Publish(ctx, name, []byte("hello"), lift.ToPartition(1))
	require.Error(t, err)
}

// Test setting and removing the readonly flag on a partition. A readonly
// partition should not accept new publications but should still be
// subscribable. This test sets only one partition as readonly and checks that
// the other one can still be published to.
func TestSetStreamReadonlySomePartitions(t *testing.T) {
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
	err = client.CreateStream(context.Background(), subject, name, lift.Partitions(2))
	require.NoError(t, err)

	// Try to set a non-existing stream as readonly.
	err = client.SetStreamReadonly(context.Background(), "bar")
	require.Error(t, err)

	// Try to set a non-existing partition as readonly.
	err = client.SetStreamReadonly(context.Background(), name, lift.ReadonlyPartitions(99))
	require.Error(t, err)

	// Set one partition as readonly.
	err = client.SetStreamReadonly(context.Background(), name, lift.ReadonlyPartitions(0))
	require.NoError(t, err)

	// Make sure setting readonly is idempotent.
	err = client.SetStreamReadonly(context.Background(), name, lift.ReadonlyPartitions(0))
	require.NoError(t, err)

	// Check that only the first partition is readonly.
	checkPartitionReadonly(t, 5*time.Second, name, 0, true, s1)
	checkPartitionReadonly(t, 5*time.Second, name, 1, false, s1)

	// Publish a message to partition 0.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Publish(ctx, name, []byte("hello"))
	require.Error(t, err)

	// Publish a message to partition 1.
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Publish(ctx, name, []byte("hello"), lift.ToPartition(1))
	require.NoError(t, err)

	msgs := make(chan *lift.Message, 1)
	ctx, cancel = context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived(), lift.Partition(1))
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, []byte("hello"), msg.Value())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	// Check that only the first partition is readonly.
	checkPartitionReadonly(t, 5*time.Second, name, 0, true, s1)
	checkPartitionReadonly(t, 5*time.Second, name, 1, false, s1)
}

// Test subscribing to a readonly stream. Stream messages should be received,
// followed by an ErrEndOfReadonlyPartition error that also ends the
// subscription.
func TestSetStreamReadonlySubscription(t *testing.T) {
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
	err = client.CreateStream(context.Background(), subject, name, lift.Partitions(2))
	require.NoError(t, err)

	// Publish a message to the stream.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Publish(ctx, name, []byte("hello"))
	require.NoError(t, err)

	// Set stream as readonly.
	err = client.SetStreamReadonly(context.Background(), name)
	require.NoError(t, err)

	msgs := make(chan *lift.Message, 1)
	ctx, cancel = context.WithCancel(context.Background())
	err = client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		// Ignore published messages, we are only waiting for the end of
		// readonly partition error.
		if err == nil {
			return
		}

		require.EqualError(t, err, lift.ErrEndOfReadonlyPartition.Error())
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the error.
	select {
	case <-msgs:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected error")
	}
}

// Ensure setting a stream readonly works when we send the request to the
// metadata follower and removing the readonly flag on the stream works when
// the SetReadonly call is sent to the follower.
func TestSetStreamReadonlyPropagate(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 0)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5050)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Connect and send the request to the follower.
	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	name := "foo"
	err = client.CreateStream(ctx, "foo", name)
	require.NoError(t, err)

	// Set the stream as readonly on follower.
	err = client.SetStreamReadonly(context.Background(), name)
	require.NoError(t, err)

	checkPartitionReadonly(t, 5*time.Second, name, 0, true, s1)
	checkPartitionReadonly(t, 5*time.Second, name, 0, true, s2)

	// Remove the readonly flag from the stream on follower.
	err = client.SetStreamReadonly(context.Background(), name, lift.Readonly(false))
	require.NoError(t, err)

	checkPartitionReadonly(t, 5*time.Second, name, 0, false, s1)
	checkPartitionReadonly(t, 5*time.Second, name, 0, false, s2)
}

// Ensure publishing to a non-existent stream returns an error.
func TestPublishNoSuchStream(t *testing.T) {
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

	_, err = client.Publish(context.Background(), "foo", []byte("hello"))
	require.Error(t, err)
}

// Ensure publishing to a non-existent stream partition returns an error.
func TestPublishNoSuchPartition(t *testing.T) {
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

	_, err = client.Publish(context.Background(), name, []byte("hello"), lift.ToPartition(42))
	require.Error(t, err)
}
