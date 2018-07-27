package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liftbridge-io/go-liftbridge"
	"github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
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
	config.Clustering.RaftBootstrap = bootstrap
	config.DataDir = filepath.Join(storagePath, id)
	config.Clustering.RaftSnapshots = 1
	config.Clustering.RaftLogging = true
	config.LogLevel = uint32(log.DebugLevel)
	config.NATS.Servers = []string{"nats://localhost:4222"}
	config.NoLog = true
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
	if leader == nil {
		stackFatalf(t, "No metadata leader found")
	}
	return leader
}

func getStreamLeader(t *testing.T, timeout time.Duration, subject, name string, servers ...*Server) *Server {
	var (
		leader   *Server
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if !s.isRunning() {
				continue
			}
			stream := s.metadata.GetStream(subject, name)
			if stream == nil {
				continue
			}
			streamLeader, _ := stream.GetLeader()
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
	s1Config := getTestConfig("a", true, 0)
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
	future := leader.raft.GetConfiguration()
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

	client, err := liftbridge.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	stream := liftbridge.StreamInfo{
		Name:              "foo",
		Subject:           "foo",
		ReplicationFactor: 1,
	}
	err = client.CreateStream(context.Background(), stream)
	require.NoError(t, err)

	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Subscribe to acks.
	acks := "acks"
	num := 5
	acked := 0
	gotAcks := make(chan struct{})
	_, err = nc.Subscribe(acks, func(m *nats.Msg) {
		_, err := liftbridge.UnmarshalAck(m.Data)
		require.NoError(t, err)
		acked++
		if acked == num {
			close(gotAcks)
		}
	})
	require.NoError(t, err)
	nc.Flush()

	// Publish some messages.
	for i := 0; i < num; i++ {
		err = nc.Publish(stream.Subject, liftbridge.NewMessage([]byte("hello"),
			liftbridge.MessageOptions{AckInbox: acks}))
		require.NoError(t, err)
	}

	// Wait for acks.
	select {
	case <-gotAcks:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected acks")
	}

	// Subscribe with overflowed offset. This should wait for new messages
	// starting at offset 5.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(5), msg.Offset)
		close(gotMsg)
		cancel()
	}, liftbridge.StartAtOffset(100))

	// Publish one more message.
	err = nc.Publish(stream.Subject, liftbridge.NewMessage([]byte("test"), liftbridge.MessageOptions{}))
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

	client, err := liftbridge.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	stream := liftbridge.StreamInfo{
		Name:              "foo",
		Subject:           "foo",
		ReplicationFactor: 1,
	}
	err = client.CreateStream(context.Background(), stream)
	require.NoError(t, err)

	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Subscribe with overflowed offset. This should wait for new messages
	// starting at offset 0.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(0), msg.Offset)
		close(gotMsg)
		cancel()
	})

	// Publish message.
	err = nc.Publish(stream.Subject, liftbridge.NewMessage([]byte("test"), liftbridge.MessageOptions{}))
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

	client, err := liftbridge.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	stream := liftbridge.StreamInfo{
		Name:              "foo",
		Subject:           "foo",
		ReplicationFactor: 1,
	}
	err = client.CreateStream(context.Background(), stream)
	require.NoError(t, err)

	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Subscribe to acks.
	acks := "acks"
	num := 2
	acked := 0
	gotAcks := make(chan struct{})
	_, err = nc.Subscribe(acks, func(m *nats.Msg) {
		_, err := liftbridge.UnmarshalAck(m.Data)
		require.NoError(t, err)
		acked++
		if acked == num {
			close(gotAcks)
		}
	})
	require.NoError(t, err)
	nc.Flush()

	// Publish some messages.
	for i := 0; i < num; i++ {
		err = nc.Publish(stream.Subject, liftbridge.NewMessage([]byte("hello"),
			liftbridge.MessageOptions{AckInbox: acks}))
		require.NoError(t, err)
	}

	// Wait for acks.
	select {
	case <-gotAcks:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected acks")
	}

	// Subscribe with underflowed offset. This should set the offset to 1.
	gotMsg := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		require.Equal(t, int64(1), msg.Offset)
		close(gotMsg)
		cancel()
	})

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

	client, err := liftbridge.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	stream := liftbridge.StreamInfo{
		Name:              "foo",
		Subject:           "foo",
		ReplicationFactor: 1,
	}
	err = client.CreateStream(context.Background(), stream)
	require.NoError(t, err)

	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Subscribe to acks.
	acks := "acks"
	num := 100
	acked := 0
	gotAcks := make(chan struct{})
	_, err = nc.Subscribe(acks, func(m *nats.Msg) {
		_, err := liftbridge.UnmarshalAck(m.Data)
		require.NoError(t, err)
		acked++
		if acked == num {
			close(gotAcks)
		}
	})
	require.NoError(t, err)
	nc.Flush()

	// Publish some messages.
	for i := 0; i < num; i++ {
		err = nc.Publish(stream.Subject, liftbridge.NewMessage([]byte("hello"),
			liftbridge.MessageOptions{AckInbox: acks}))
		require.NoError(t, err)
	}

	// Wait for acks.
	select {
	case <-gotAcks:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected acks")
	}

	// The first message read back should have offset 85.
	msgs := make(chan *proto.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	})

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, int64(85), msg.Offset)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}
