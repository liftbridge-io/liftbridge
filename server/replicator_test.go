package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/liftbridge-io/liftbridge-grpc/go"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/liftbridge-io/liftbridge/server/commitlog"
	internal "github.com/liftbridge-io/liftbridge/server/proto"
)

func waitForHW(t *testing.T, timeout time.Duration, name string, partitionID int32, hw int64, servers ...*Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			partition := s.metadata.GetPartition(name, partitionID)
			if partition == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
			if partition.log.HighWatermark() < hw {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
		}
		return
	}
	stackFatalf(t, "Cluster did not reach HW %d for [name=%s, partition=%d]", hw, name, partitionID)
}

func waitForPartition(t *testing.T, timeout time.Duration, name string, partitionID int32, servers ...*Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			partition := s.metadata.GetPartition(name, partitionID)
			if partition == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
		}
		return
	}
	stackFatalf(t, "Cluster did not create partition [name=%s, partition=%d]", name, partitionID)
}

func waitForISR(t *testing.T, timeout time.Duration, name string, partitionID int32, isrSize int, servers ...*Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			partition := s.metadata.GetPartition(name, partitionID)
			if partition == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
			if partition.ISRSize() != isrSize {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
		}
		return
	}
	stackFatalf(t, "Cluster did not reach ISR size %d for [name=%s, partition=%d]", isrSize, name, partitionID)
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
	s1Config.Clustering.ReplicaMaxLeaderTimeout = time.Second
	s1Config.Clustering.ReplicaMaxIdleWait = 500 * time.Millisecond
	s1Config.Clustering.ReplicaFetchTimeout = 500 * time.Millisecond
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2Config.Clustering.ReplicaMaxLeaderTimeout = time.Second
	s2Config.Clustering.ReplicaMaxIdleWait = 500 * time.Millisecond
	s2Config.Clustering.ReplicaFetchTimeout = 500 * time.Millisecond
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Configure second server.
	s3Config := getTestConfig("c", false, 5052)
	s3Config.Clustering.ReplicaMaxLeaderTimeout = time.Second
	s3Config.Clustering.ReplicaMaxIdleWait = 500 * time.Millisecond
	s3Config.Clustering.ReplicaFetchTimeout = 500 * time.Millisecond
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
	err = client.Subscribe(context.Background(), name,
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
	waitForHW(t, 5*time.Second, name, 0, int64(num-1), servers...)

	// Kill the stream leader.
	leader := getPartitionLeader(t, 10*time.Second, name, 0, servers...)
	leader.Stop()
	followers := []*Server{}
	for _, s := range servers {
		if s == leader {
			continue
		}
		followers = append(followers, s)
	}

	// Wait for new leader to be elected.
	getPartitionLeader(t, 10*time.Second, name, 0, followers...)

	// Make sure the new leader's log is consistent.
	i = 0
	ch = make(chan struct{})
	err = client.Subscribe(context.Background(), name,
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
	s1Config.Clustering.ReplicaFetchTimeout = 100 * time.Millisecond
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2Config.Clustering.ReplicaMaxLagTime = time.Second
	s2Config.Clustering.ReplicaFetchTimeout = 100 * time.Millisecond
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Configure third server.
	s3Config := getTestConfig("c", false, 5052)
	s3Config.Clustering.ReplicaMaxLagTime = time.Second
	s3Config.Clustering.ReplicaFetchTimeout = 100 * time.Millisecond
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
	leader = getPartitionLeader(t, 10*time.Second, name, 0, servers...)
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
	leader = getPartitionLeader(t, 10*time.Second, name, 0, servers...)
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
	leader = getPartitionLeader(t, 10*time.Second, name, 0, servers...)
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
	getPartitionLeader(t, 10*time.Second, name, 0, leader, follower)

	// Ensure all messages have been committed by reading them back.
	i := 0
	ch := make(chan struct{})
	err = client.Subscribe(context.Background(), name,
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

// Ensure messages aren't lost when a follower restarts (and truncates its log)
// and then immediately becomes the leader.
func TestTruncateFastLeaderElection(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.Clustering.MinISR = 1
	s1Config.Clustering.ReplicaMaxLeaderTimeout = time.Second
	s1Config.Clustering.ReplicaMaxIdleWait = 500 * time.Millisecond
	s1Config.Clustering.ReplicaFetchTimeout = 500 * time.Millisecond
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2Config.Clustering.MinISR = 1
	s2Config.Clustering.ReplicaMaxLeaderTimeout = time.Second
	s2Config.Clustering.ReplicaMaxIdleWait = 500 * time.Millisecond
	s2Config.Clustering.ReplicaFetchTimeout = 500 * time.Millisecond
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Configure third server.
	s3Config := getTestConfig("c", false, 5052)
	s3Config.Clustering.MinISR = 1
	s3Config.Clustering.ReplicaMaxLeaderTimeout = time.Second
	s3Config.Clustering.ReplicaMaxIdleWait = 500 * time.Millisecond
	s3Config.Clustering.ReplicaFetchTimeout = 500 * time.Millisecond
	s3 := runServerWithConfig(t, s3Config)
	defer s3.Stop()

	servers := []*Server{s1, s2, s3}
	getMetadataLeader(t, 10*time.Second, servers...)

	client, err := lift.Connect([]string{"localhost:5050", "localhost:5051", "localhost:5052"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name,
		lift.ReplicationFactor(3))
	require.NoError(t, err)

	// Publish two messages.
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Publish(ctx, subject, []byte("hello"), lift.AckPolicyAll())
	require.NoError(t, err)
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Publish(ctx, subject, []byte("world"), lift.AckPolicyAll())
	require.NoError(t, err)

	// Find stream followers.
	leader := getPartitionLeader(t, 10*time.Second, name, 0, servers...)
	var (
		follower1 *Server
		follower2 *Server
	)
	if leader == s1 {
		follower1 = s2
		follower2 = s3
	} else if leader == s2 {
		follower1 = s1
		follower2 = s3
	} else {
		follower1 = s1
		follower2 = s2
	}

	// At this point, all servers should have a HW of 1. Set the followers'
	// HW to 0 to simulate a follower updating its HW from the leader (also
	// disable replication to prevent them from advancing their HW from the
	// leader).
	waitForHW(t, 5*time.Second, name, 0, 1, servers...)

	// Stop first follower's replication and reset HW.
	partition1 := follower1.metadata.GetPartition(name, 0)
	require.NotNil(t, partition1)
	require.NoError(t, partition1.stopFollowing())
	partition1.log.(*commitlog.CommitLog).OverrideHighWatermark(0)

	// Stop second follower's replication and reset HW.
	partition2 := follower2.metadata.GetPartition(name, 0)
	require.NotNil(t, partition2)
	require.NoError(t, partition2.stopFollowing())
	partition2.log.(*commitlog.CommitLog).OverrideHighWatermark(0)

	var (
		follower1Config *Config
		follower2Config *Config
	)
	if leader == s1 {
		follower1Config = s2Config
		follower2Config = s3Config
	} else if leader == s2 {
		follower1Config = s1Config
		follower2Config = s3Config
	} else {
		follower1Config = s1Config
		follower2Config = s2Config
	}

	// Restart the first follower (this will truncate uncommitted messages).
	follower1.Stop()
	follower1 = runServerWithConfig(t, follower1Config)
	defer follower1.Stop()

	// Restart the second follower (this will truncate uncommitted messages).
	follower2.Stop()
	follower2 = runServerWithConfig(t, follower2Config)
	defer follower2.Stop()

	// Stop replication on the leader to force a leader election.
	partition := leader.metadata.GetPartition(name, 0)
	require.NotNil(t, partition)
	partition.pauseReplication()

	// Wait for stream leader to be elected.
	leader = getPartitionLeader(t, 10*time.Second, name, 0, follower1, follower2)

	// Ensure messages have not been lost.
	partition = leader.metadata.GetPartition(name, 0)
	require.NotNil(t, partition)
	require.Equal(t, int64(0), partition.log.OldestOffset())
	require.Equal(t, int64(1), partition.log.NewestOffset())
}

// Ensure log lineages don't diverge in the event of multiple hard failures.
func TestTruncatePreventReplicaDivergence(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.Clustering.MinISR = 1
	s1Config.Clustering.ReplicaMaxLeaderTimeout = time.Second
	s1Config.Clustering.ReplicaMaxIdleWait = 500 * time.Millisecond
	s1Config.Clustering.ReplicaFetchTimeout = 500 * time.Millisecond
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2Config.Clustering.MinISR = 1
	s2Config.Clustering.ReplicaMaxLeaderTimeout = time.Second
	s2Config.Clustering.ReplicaMaxIdleWait = 500 * time.Millisecond
	s2Config.Clustering.ReplicaFetchTimeout = 500 * time.Millisecond
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Configure third server.
	s3Config := getTestConfig("c", false, 5052)
	s3Config.Clustering.MinISR = 1
	s3Config.Clustering.ReplicaMaxLeaderTimeout = time.Second
	s3Config.Clustering.ReplicaMaxIdleWait = 500 * time.Millisecond
	s3Config.Clustering.ReplicaFetchTimeout = 500 * time.Millisecond
	s3 := runServerWithConfig(t, s3Config)
	defer s3.Stop()

	servers := []*Server{s1, s2, s3}
	getMetadataLeader(t, 10*time.Second, servers...)

	client, err := lift.Connect([]string{"localhost:5050", "localhost:5051", "localhost:5052"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name,
		lift.ReplicationFactor(3))
	require.NoError(t, err)

	// Publish two messages.
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Publish(ctx, subject, []byte("hello"))
	require.NoError(t, err)
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Publish(ctx, subject, []byte("world"))
	require.NoError(t, err)

	// Find stream followers.
	leader := getPartitionLeader(t, 10*time.Second, name, 0, servers...)
	var (
		follower1 *Server
		follower2 *Server
	)
	if leader == s1 {
		follower1 = s2
		follower2 = s3
	} else if leader == s2 {
		follower1 = s1
		follower2 = s3
	} else {
		follower1 = s1
		follower2 = s2
	}

	// At this point, all servers should have a HW of 1. Set the followers'
	// HW to 0 to simulate a follower crashing before replicating (also
	// disable replication to prevent them from advancing their HW from the
	// leader).
	waitForHW(t, 5*time.Second, name, 0, 1, servers...)

	// Stop first follower's replication and reset HW.
	partition1 := follower1.metadata.GetPartition(name, 0)
	require.NotNil(t, partition1)
	partition1.mu.Lock()
	require.NoError(t, partition1.stopFollowing())
	partition1.mu.Unlock()
	partition1.log.(*commitlog.CommitLog).OverrideHighWatermark(0)
	partition1.truncateToHW()

	// Stop second follower's replication and reset HW.
	partition2 := follower2.metadata.GetPartition(name, 0)
	require.NotNil(t, partition2)
	partition2.mu.Lock()
	require.NoError(t, partition2.stopFollowing())
	partition2.mu.Unlock()
	partition2.log.(*commitlog.CommitLog).OverrideHighWatermark(0)
	partition2.truncateToHW()

	var (
		oldLeaderConfig *Config
		follower1Config *Config
		follower2Config *Config
	)
	if leader == s1 {
		oldLeaderConfig = s1Config
		follower1Config = s2Config
		follower2Config = s3Config
	} else if leader == s2 {
		oldLeaderConfig = s2Config
		follower1Config = s1Config
		follower2Config = s3Config
	} else {
		oldLeaderConfig = s3Config
		follower1Config = s1Config
		follower2Config = s2Config
	}

	// Stop replication on the leader to force a leader election.
	partition := leader.metadata.GetPartition(name, 0)
	require.NotNil(t, partition)
	partition.pauseReplication()

	// Restart the first follower (this will truncate uncommitted messages).
	follower1.Stop()
	follower1Config.Clustering.ReplicaMaxLagTime = 2 * time.Second
	follower1 = runServerWithConfig(t, follower1Config)
	defer follower1.Stop()

	// Restart the second follower (this will truncate uncommitted messages).
	follower2.Stop()
	follower2Config.Clustering.ReplicaMaxLagTime = 2 * time.Second
	follower2 = runServerWithConfig(t, follower2Config)
	defer follower2.Stop()

	// Wait for stream leader to be elected.
	getPartitionLeader(t, 10*time.Second, name, 0, follower1, follower2)

	// Stop the old leader.
	leader.Stop()

	// Wait for ISR to shrink.
	waitForISR(t, 10*time.Second, name, 0, 2, follower1, follower2)

	// Publish new messages.
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Publish(ctx, subject, []byte("goodnight"))
	require.NoError(t, err)

	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Publish(ctx, subject, []byte("moon"))
	require.NoError(t, err)

	// Restart old leader.
	oldLeader := runServerWithConfig(t, oldLeaderConfig)
	defer oldLeader.Stop()

	// Wait for HW to update.
	servers = []*Server{follower1, follower2, oldLeader}
	waitForHW(t, 5*time.Second, name, 0, 2, servers...)

	// Ensure log lineages have not diverged.
	for _, s := range servers {
		partition := s.metadata.GetPartition(name, 0)
		require.NotNil(t, partition)
		require.Equal(t, int64(0), partition.log.OldestOffset())
		require.Equal(t, int64(2), partition.log.NewestOffset())

		reader, err := partition.log.NewReader(0, false)
		require.NoError(t, err)
		headersBuf := make([]byte, 28)

		msg, offset, _, _, err := reader.ReadMessage(context.Background(), headersBuf)
		require.NoError(t, err)
		require.Equal(t, int64(0), offset)
		require.Equal(t, []byte("hello"), msg.Value())

		// The second message we published was orphaned and should have been
		// truncated.

		msg, offset, _, _, err = reader.ReadMessage(context.Background(), headersBuf)
		require.NoError(t, err)
		require.Equal(t, int64(1), offset)
		require.Equal(t, []byte("goodnight"), msg.Value())

		msg, offset, _, _, err = reader.ReadMessage(context.Background(), headersBuf)
		require.NoError(t, err)
		require.Equal(t, int64(2), offset)
		require.Equal(t, []byte("moon"), msg.Value())
	}
}

// Ensure when a follower has caught up with the leader's log, the leader
// notifies the follower when new messages are written to the partition.
func TestReplicatorNotifyNewData(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Configure first server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	getMetadataLeader(t, 10*time.Second, s1, s2)

	client, err := lift.Connect([]string{"localhost:5050", "localhost:5051"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name,
		lift.ReplicationFactor(2))
	require.NoError(t, err)

	// Get partition leader.
	leader := getPartitionLeader(t, 5*time.Second, name, 0, s1, s2)
	var follower *Server
	if leader == s1 {
		follower = s2
	} else {
		follower = s1
	}

	// At this point, the follower is caught up with the leader since there
	// aren't any messages. Set up a NATS subscription to intercept
	// notifications.
	var (
		notifications = make(chan *internal.PartitionNotification)
		inbox         = follower.getPartitionNotificationInbox(
			follower.config.Clustering.ServerID)
	)
	_, err = nc.Subscribe(inbox, func(msg *nats.Msg) {
		req := &internal.PartitionNotification{}
		if err := req.Unmarshal(msg.Data); err != nil {
			t.Fatalf("Invalid partition notification: %v", err)
		}
		notifications <- req
	})
	require.NoError(t, err)

	// Publish a message. This will cause a notification to be sent to the
	// follower.
	_, err = client.Publish(context.Background(), subject, []byte("hello"))
	require.NoError(t, err)

	select {
	case req := <-notifications:
		require.Equal(t, name, req.Stream)
		require.Equal(t, int32(0), req.Partition)
	case <-time.After(5 * time.Second):
		t.Fatal("Expected partition notification")
	}
}
