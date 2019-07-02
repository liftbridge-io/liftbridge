package server

import (
	"strconv"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func waitForHW(t *testing.T, timeout time.Duration, subject, name string, hw int64, servers ...*Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			stream := s.metadata.GetStream(subject, name)
			if stream == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
			if stream.log.HighWatermark() < hw {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
		}
		return
	}
	stackFatalf(t, "Cluster did not reach HW %d for [subject=%s, name=%s]", hw, subject, name)
}

func waitForStream(t *testing.T, timeout time.Duration, subject, name string, servers ...*Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			stream := s.metadata.GetStream(subject, name)
			if stream == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
		}
		return
	}
	stackFatalf(t, "Cluster did not create stream [subject=%s, name=%s]", subject, name)
}

// Ensure messages are replicated and the stream leader fails over when the
// leader dies.
func TestStreamLeaderFailover(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.Clustering.ReplicaMaxLeaderTimeout = 2 * time.Second
	s1Config.Clustering.ReplicaFetchTimeout = time.Second
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2Config.Clustering.ReplicaMaxLeaderTimeout = 2 * time.Second
	s2Config.Clustering.ReplicaFetchTimeout = time.Second
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Configure second server.
	s3Config := getTestConfig("c", false, 5052)
	s3Config.Clustering.ReplicaMaxLeaderTimeout = 2 * time.Second
	s3Config.Clustering.ReplicaFetchTimeout = time.Second
	s3 := runServerWithConfig(t, s3Config)
	defer s3.Stop()

	servers := []*Server{s1, s2, s3}
	getMetadataLeader(t, 10*time.Second, servers...)

	client, err := lift.Connect([]string{"localhost:5050", "localhost:5051", "localhost:5052"})
	require.NoError(t, err)
	defer client.Close()

	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name,
		lift.ReplicationFactor(3))
	require.NoError(t, err)

	num := 100
	expected := make([]*message, num)
	for i := 0; i < num; i++ {
		expected[i] = &message{
			Key:    []byte("bar"),
			Value:  []byte(strconv.Itoa(i)),
			Offset: int64(i),
		}
	}

	// Publish messages.
	for i := 0; i < num; i++ {
		_, err := client.Publish(context.Background(), subject, expected[i].Value,
			lift.Key(expected[i].Key), lift.AckPolicyAll())
		require.NoError(t, err)
	}

	// Make sure we can play back the log.
	i := 0
	ch := make(chan struct{})
	err = client.Subscribe(context.Background(), subject, name,
		func(msg *proto.Message, err error) {
			if i == num && err != nil {
				return
			}
			require.NoError(t, err)
			expect := expected[i]
			assertMsg(t, expect, msg)
			i++
			if i == num {
				close(ch)
			}
		}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}

	// Wait for HW to update on followers.
	waitForHW(t, 5*time.Second, subject, name, int64(num-1), servers...)

	// Kill the stream leader.
	leader := getStreamLeader(t, 10*time.Second, subject, name, servers...)
	leader.Stop()
	followers := []*Server{}
	for _, s := range servers {
		if s == leader {
			continue
		}
		followers = append(followers, s)
	}

	// Wait for new leader to be elected.
	getStreamLeader(t, 10*time.Second, subject, name, followers...)

	// Make sure the new leader's log is consistent.
	i = 0
	ch = make(chan struct{})
	err = client.Subscribe(context.Background(), subject, name,
		func(msg *proto.Message, err error) {
			if i == num && err != nil {
				return
			}
			require.NoError(t, err)
			expect := expected[i]
			assertMsg(t, expect, msg)
			i++
			if i == num {
				close(ch)
			}
		}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}
}

// Ensure the leader commits when the ISR shrinks if it causes pending messages
// to now be replicated by all replicas in ISR.
func TestCommitOnISRShrink(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.Clustering.ReplicaMaxLagTime = time.Second
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2Config.Clustering.ReplicaMaxLagTime = time.Second
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Configure third server.
	s3Config := getTestConfig("c", false, 5052)
	s3Config.Clustering.ReplicaMaxLagTime = time.Second
	s3 := runServerWithConfig(t, s3Config)
	defer s3.Stop()

	servers := []*Server{s1, s2, s3}
	leader := getMetadataLeader(t, 10*time.Second, servers...)

	client, err := lift.Connect([]string{"localhost:5050", "localhost:5051", "localhost:5052"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name,
		lift.ReplicationFactor(3))
	require.NoError(t, err)

	// Kill a stream follower.
	leader = getStreamLeader(t, 10*time.Second, subject, name, servers...)
	var follower *Server
	for i, server := range servers {
		if server != leader {
			follower = server
			servers = append(servers[:i], servers[i+1:]...)
			break
		}
	}
	follower.Stop()

	// Publish message to stream. This should not get committed until the ISR
	// shrinks.
	gotAck := make(chan error)
	go func() {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Publish(ctx, subject, []byte("hello"), lift.AckPolicyAll())
		gotAck <- err
	}()

	// Ensure we don't receive an ack yet.
	select {
	case <-gotAck:
		t.Fatal("Received unexpected ack")
	case <-time.After(500 * time.Millisecond):
	}

	// Eventually, the ISR should shrink and we should receive an ack.
	select {
	case <-gotAck:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive expected ack")
	}
}

// Ensure an ack is received even if there is a server not responding in the
// ISR if AckPolicy_LEADER is set.
func TestAckPolicyLeader(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Configure third server.
	s3Config := getTestConfig("c", false, 5052)
	s3 := runServerWithConfig(t, s3Config)
	defer s3.Stop()

	servers := []*Server{s1, s2, s3}
	leader := getMetadataLeader(t, 10*time.Second, servers...)

	client, err := lift.Connect([]string{"localhost:5050", "localhost:5051", "localhost:5052"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name,
		lift.ReplicationFactor(3))
	require.NoError(t, err)

	// Kill a stream follower.
	leader = getStreamLeader(t, 10*time.Second, subject, name, servers...)
	var follower *Server
	for i, server := range servers {
		if server != leader {
			follower = server
			servers = append(servers[:i], servers[i+1:]...)
			break
		}
	}
	follower.Stop()

	// Publish message to stream. This should not get committed until the ISR
	// shrinks, but an ack should still be received immediately since
	// AckPolicy_LEADER is set (default AckPolicy).
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	cid := "cid"
	ack, err := client.Publish(ctx, subject, []byte("hello"),
		lift.CorrelationID(cid))
	require.NoError(t, err)
	require.NotNil(t, ack)
	require.Equal(t, cid, ack.CorrelationId)
}

// Ensure messages in the log still get committed after the leader is
// restarted.
func TestCommitOnRestart(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.Clustering.MinISR = 2
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2Config.Clustering.MinISR = 2
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	servers := []*Server{s1, s2}
	leader := getMetadataLeader(t, 10*time.Second, servers...)

	client, err := lift.Connect([]string{"localhost:5050", "localhost:5051"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name,
		lift.ReplicationFactor(2))
	require.NoError(t, err)

	// Publish some messages.
	num := 5
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"), lift.AckPolicyAll())
		require.NoError(t, err)
	}

	// Kill stream follower.
	leader = getStreamLeader(t, 10*time.Second, subject, name, servers...)
	var follower *Server
	for i, server := range servers {
		if server != leader {
			follower = server
			servers = append(servers[:i], servers[i+1:]...)
			break
		}
	}
	follower.Stop()

	// Publish some more messages.
	for i := 0; i < num; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Publish(ctx, subject, []byte("hello"))
		require.NoError(t, err)
	}

	var (
		leaderConfig   *Config
		followerConfig *Config
	)
	if leader == s1 {
		leaderConfig = s1Config
		followerConfig = s2Config
	} else {
		leaderConfig = s2Config
		followerConfig = s1Config
	}

	// Restart the leader.
	leader.Stop()
	leader = runServerWithConfig(t, leaderConfig)
	defer leader.Stop()

	// Bring the follower back up.
	follower = runServerWithConfig(t, followerConfig)
	defer follower.Stop()

	// Wait for stream leader to be elected.
	getStreamLeader(t, 10*time.Second, subject, name, leader, follower)

	// Ensure all messages have been committed by reading them back.
	i := 0
	ch := make(chan struct{})
	err = client.Subscribe(context.Background(), subject, name,
		func(msg *proto.Message, err error) {
			if i == num*2 && err != nil {
				return
			}
			require.NoError(t, err)
			require.Equal(t, int64(i), msg.Offset)
			i++
			if i == num*2 {
				close(ch)
			}
		}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}
}
