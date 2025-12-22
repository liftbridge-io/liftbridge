package server

import (
	"context"
	"testing"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	client "github.com/liftbridge-io/liftbridge-api/v2/go"
	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

func createServer() *Server {
	config := getTestConfig("a", true, 0)
	return New(config)
}

func waitForCommitQueue(t *testing.T, timeout time.Duration, size int64, partition *partition) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if partition.commitQueue.Len() == size {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	stackFatalf(t, "Commit queue did not reach size %d", size)
}

func waitForPause(t *testing.T, timeout time.Duration, partition *partition) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if partition.IsPaused() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	stackFatalf(t, "Partition did not pause in time")
}

// Ensure commitLoop commits messages in the queue when they have been
// replicated by all replicas in the ISR. This is done by removing committed
// messages and updating the HW. Ensure acks are not sent when the AckPolicy is
// not AckPolicy_ALL.
func TestPartitionCommitLoopCommitNoAck(t *testing.T) {
	defer cleanupStorage(t)

	// Start Liftbridge server.
	server := createServer()
	require.NoError(t, server.Start())
	defer server.Stop()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	p.commitQueue = queue.New(5)

	// Subscribe to ack inbox.
	ackInbox := "ack"
	sub, err := nc.SubscribeSync(ackInbox)
	require.NoError(t, err)
	nc.Flush()

	// Put some messages in the queue.
	p.commitQueue.Put(&client.Ack{Offset: 0, AckInbox: ackInbox})
	p.commitQueue.Put(&client.Ack{Offset: 1, AckInbox: ackInbox})
	p.commitQueue.Put(&client.Ack{Offset: 2, AckInbox: ackInbox})

	// Mark 0 and 1 as fully replicated.
	p.isr["a"].offset = 1
	p.isr["b"].offset = 2

	// Start commit loop.
	stop := make(chan struct{})
	go p.commitLoop(stop)

	// Trigger a commit.
	p.commitCheck <- struct{}{}

	// Wait for messages to be committed.
	waitForCommitQueue(t, 5*time.Second, 1, p)

	// Stop the loop.
	close(stop)

	// Verify HW.
	require.Equal(t, int64(1), p.log.HighWatermark())

	// Ensure no acks were published.
	_, err = sub.NextMsg(500 * time.Millisecond)
	require.Error(t, err)
}

// Ensure commitLoop commits messages in the queue when they have been
// replicated by all replicas in the ISR. This is done by removing committed
// messages and updating the HW. Ensure acks are sent when the AckPolicy is
// AckPolicy_ALL.
func TestPartitionCommitLoopCommitAck(t *testing.T) {
	defer cleanupStorage(t)

	// Start Liftbridge server.
	server := createServer()
	require.NoError(t, server.Start())
	defer server.Stop()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	p.commitQueue = queue.New(5)

	// Subscribe to ack inbox.
	ackInbox := "ack"
	sub, err := nc.SubscribeSync(ackInbox)
	require.NoError(t, err)
	nc.Flush()

	// Put some messages in the queue.
	p.commitQueue.Put(&client.Ack{Offset: 0, AckInbox: ackInbox, AckPolicy: client.AckPolicy_ALL})
	p.commitQueue.Put(&client.Ack{Offset: 1, AckInbox: ackInbox})
	p.commitQueue.Put(&client.Ack{Offset: 2, AckInbox: ackInbox, AckPolicy: client.AckPolicy_ALL})

	// Mark messages as fully replicated.
	p.isr["a"].offset = 2
	p.isr["b"].offset = 2

	// Start commit loop.
	stop := make(chan struct{})
	go p.commitLoop(stop)

	// Trigger a commit.
	p.commitCheck <- struct{}{}

	// Wait for messages to be committed.
	waitForCommitQueue(t, 5*time.Second, 0, p)

	// Stop the loop.
	close(stop)

	// Verify HW.
	require.Equal(t, int64(2), p.log.HighWatermark())

	// Ensure acks were published.
	_, err = sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	_, err = sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
}

// Ensure commitLoop is a no-op when the commitQueue is empty.
func TestPartitionCommitLoopEmptyQueue(t *testing.T) {
	defer cleanupStorage(t)

	// Start Liftbridge server.
	server := createServer()
	require.NoError(t, server.Start())
	defer server.Stop()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	p.commitQueue = queue.New(5)

	// Subscribe to ack inbox.
	ackInbox := "ack"
	sub, err := nc.SubscribeSync(ackInbox)
	require.NoError(t, err)
	nc.Flush()

	// Start commit loop.
	stop := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		p.commitLoop(stop)
		close(exited)
	}()

	// Trigger a commit.
	p.commitCheck <- struct{}{}

	// Stop the loop.
	close(stop)

	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected commit loop to exit")
	}

	// Verify HW.
	require.Equal(t, int64(-1), p.log.HighWatermark())

	// Ensure no acks were published.
	_, err = sub.NextMsg(500 * time.Millisecond)
	require.Error(t, err)
}

// Ensure commitLoop exits when the queue is disposed.
func TestPartitionCommitLoopDisposedQueue(t *testing.T) {
	defer cleanupStorage(t)

	// Start Liftbridge server.
	server := createServer()
	require.NoError(t, server.Start())
	defer server.Stop()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	p.commitQueue = queue.New(5)

	// Subscribe to ack inbox.
	ackInbox := "ack"
	sub, err := nc.SubscribeSync(ackInbox)
	require.NoError(t, err)
	nc.Flush()

	// Put some messages in the queue.
	p.commitQueue.Put(&client.Ack{Offset: 0, AckInbox: ackInbox})
	p.commitQueue.Put(&client.Ack{Offset: 1, AckInbox: ackInbox})
	p.commitQueue.Put(&client.Ack{Offset: 2, AckInbox: ackInbox})

	// Mark 0 and 1 as fully replicated.
	p.isr["a"].offset = 1
	p.isr["b"].offset = 2

	// Start commit loop.
	exited := make(chan struct{})
	go func() {
		p.commitLoop(make(chan struct{}))
		close(exited)
	}()

	// Dispose commit queue.
	p.commitQueue.Dispose()

	// Trigger a commit.
	p.commitCheck <- struct{}{}

	// Verify HW.
	require.Equal(t, int64(-1), p.log.HighWatermark())

	// Ensure no acks were published.
	_, err = sub.NextMsg(500 * time.Millisecond)
	require.Error(t, err)

	// Ensure loop exited.
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected commit loop to exit")
	}
}

// Ensure commitLoop does not commit messages in the queue if the ISR is below
// the minimum ISR size.
func TestPartitionCommitLoopNoCommitBelowMinISR(t *testing.T) {
	defer cleanupStorage(t)

	server := createServer()
	server.config.Clustering.MinISR = 2
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	p.commitQueue = queue.New(5)
	require.NoError(t, p.RemoveFromISR("b"))

	// Put some messages in the queue.
	p.commitQueue.Put(&client.Ack{Offset: 0})
	p.commitQueue.Put(&client.Ack{Offset: 1})
	p.commitQueue.Put(&client.Ack{Offset: 2})

	// Mark 0 and 1 as fully replicated.
	p.isr["a"].offset = 1

	// Start commit loop.
	stop := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		p.commitLoop(stop)
		close(exited)
	}()

	// Trigger a commit.
	p.commitCheck <- struct{}{}

	// Stop the loop.
	close(stop)

	// Ensure loop exited.
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected commit loop to exit")
	}

	// Verify nothing was committed.
	require.Equal(t, int64(3), p.commitQueue.Len())
	require.Equal(t, int64(-1), p.log.HighWatermark())
}

// Ensure RemoveFromISR returns an error if the replica is not a stream
// replica.
func TestPartitionRemoveFromISRNotReplica(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer()
	p, err := server.newPartition(&proto.Partition{
		Subject: "foo",
		Stream:  "foo",
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	require.Error(t, p.RemoveFromISR("foo"))
}

// Ensure RemoveFromISR removes the replica from the ISR and does not trigger a
// commit check on the follower.
func TestPartitionRemoveFromISRFollower(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer()
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b", "c"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.RemoveFromISR("b"))

	// Verify there is no commit check.
	select {
	case <-p.commitCheck:
		t.Fatal("Unexpected commit check")
	default:
	}

	require.ElementsMatch(t, []string{"a", "c"}, p.GetISR())
	require.False(t, p.belowMinISR)
}

// Ensure RemoveFromISR removes the replica from the ISR and triggers a commit
// check on the leader.
func TestPartitionRemoveFromISRLeader(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer()
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "a",
		Isr:      []string{"a", "b", "c"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	p.isLeading = true
	require.NoError(t, p.RemoveFromISR("b"))

	// Verify there is a commit check.
	select {
	case <-p.commitCheck:
	default:
		t.Fatal("Did not get expected commit check")
	}

	require.ElementsMatch(t, []string{"a", "c"}, p.GetISR())
	require.False(t, p.belowMinISR)

	p.isLeading = false
}

// Ensure RemoveFromISR removes the replica from the ISR and marks the stream
// as below the minimum ISR when the ISR shrinks below the minimum.
func TestPartitionRemoveFromISRBelowMin(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer()
	server.config.Clustering.MinISR = 3
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b", "c"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.RemoveFromISR("b"))

	require.ElementsMatch(t, []string{"a", "c"}, p.GetISR())
	require.True(t, p.belowMinISR)
}

// Ensure AddToISR returns an error if the replica is not a stream replica.
func TestPartitionAddToISRNotReplica(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer()
	p, err := server.newPartition(&proto.Partition{
		Subject: "foo",
		Stream:  "foo",
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	require.Error(t, p.AddToISR("foo"))
}

// Ensure AddToISR adds the replica to the ISR.
func TestPartitionAddToISR(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer()
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()

	require.ElementsMatch(t, []string{"a", "b"}, p.GetISR())

	require.NoError(t, p.AddToISR("c"))

	require.ElementsMatch(t, []string{"a", "b", "c"}, p.GetISR())
}

// Ensure AddToISR adds the replica to the ISR and, if the stream was below the
// minimum ISR and has recovered, marks the stream ISR as recovered.
func TestPartitionAddToISRRecoverMin(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer()
	server.config.Clustering.MinISR = 3
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()
	p.belowMinISR = true

	require.ElementsMatch(t, []string{"a", "b"}, p.GetISR())

	require.NoError(t, p.AddToISR("c"))

	require.ElementsMatch(t, []string{"a", "b", "c"}, p.GetISR())
	require.False(t, p.belowMinISR)
}

// Ensure replicationRequestLoop's idle follower sleep is preempted when a
// partition notification is received.
func TestPartitionReplicationRequestLoopPreempt(t *testing.T) {
	defer cleanupStorage(t)

	// Start Liftbridge server.
	config := getTestConfig("a", true, 5050)
	// Set idle wait long enough that it shouldn't be reached.
	config.Clustering.ReplicaMaxIdleWait = time.Hour
	server := runServerWithConfig(t, config)
	defer server.Stop()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "b",
		Isr:      []string{"a", "b"},
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()

	leaderEpoch := uint64(1)

	// Set up mock leader.
	requests := make(chan struct{}, 2)
	_, err = nc.Subscribe(p.getReplicationRequestInbox(), func(msg *nats.Msg) {
		// Send empty replication response.
		resp := make([]byte, 16)
		proto.Encoding.PutUint64(resp, leaderEpoch)
		proto.Encoding.PutUint64(resp[8:], 0)
		msg.Respond(resp)
		requests <- struct{}{}
	})
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	stop := make(chan struct{})
	defer close(stop)
	go p.replicationRequestLoop("b", leaderEpoch, stop)

	select {
	case <-requests:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected replication request")
	}

	// Notify partition to preempt sleep.
	p.Notify()

	select {
	case <-requests:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected replication request")
	}
}

// Ensure that a new partition can be created with custom StreamConfig.
func TestPartitionWithCustomConfigNoError(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer()
	customStreamConfig := &proto.StreamConfig{
		RetentionMaxMessages: &proto.NullableInt64{Value: 1000},
	}
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b"},
	}, false, customStreamConfig)
	require.NoError(t, err)
	defer p.Close()
}

// Ensure when streams.auto.pause.time is enabled, partitions automatically
// pause when idle.
func TestPartitionAutoPause(t *testing.T) {
	defer cleanupStorage(t)

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	autoPauseTime := 100 * time.Millisecond
	s1Config.Streams.AutoPauseTime = autoPauseTime
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	subject := "foo"
	name := "foo"

	// Start publishing to stream subject to keep it active.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			nc.Publish(subject, nil)
		}
	}()

	// Create stream
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Stop publishing to trigger a pause.
	close(stop)
	before := time.Now()

	waitForPause(t, 2*time.Second, s1.metadata.GetPartition(name, 0))
	require.True(t, time.Since(before) > autoPauseTime)
}

// Ensure when streams.auto.pause.disable.if.subscribers is enabled, partitions
// automatically pause when idle only if there is no subscriber.
func TestPartitionAutoPauseDisableIfSubscribers(t *testing.T) {
	defer cleanupStorage(t)

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	autoPauseTime := 100 * time.Millisecond
	s1Config.Streams.AutoPauseTime = autoPauseTime
	s1Config.Streams.AutoPauseDisableIfSubscribers = true
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	subject := "foo"
	name := "foo"

	// Start publishing to stream subject to keep it active.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			nc.Publish(subject, nil)
		}
	}()

	// Create stream
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	// Subscribe to the stream.
	err = client.Subscribe(context.Background(), name, func(msg *lift.Message, err error) {})
	require.NoError(t, err)

	// Stop publishing.
	close(stop)

	// Wait some time.
	time.Sleep(200 * time.Millisecond)

	// Check that the partition has not been paused.
	partition := s1.metadata.GetPartition(name, 0)
	require.False(t, partition.IsPaused())

	waitForPartition(t, time.Second, name, 0)
}

// Ensure computeTick correctly computes the sleep time for the tick loop based
// on the elapsed time.
func TestComputeTick(t *testing.T) {
	maxSleep := 10 * time.Second

	require.Equal(t, time.Duration(0), computeTick(maxSleep, maxSleep))

	require.Equal(t, time.Second, computeTick(9*time.Second, maxSleep))

	require.Equal(t, 9*time.Second, computeTick(time.Second, maxSleep))

	require.Equal(t, maxSleep, computeTick(15*time.Second, maxSleep))

	require.Equal(t, maxSleep, computeTick(0, maxSleep))
}

// Ensure becomeLeader handles the case where the leader is not in the ISR
// gracefully by adding self to ISR instead of panicking. This can happen
// when restoring from a corrupt Raft snapshot. See issue #354.
func TestPartitionBecomeLeaderNotInISR(t *testing.T) {
	defer cleanupStorage(t)

	// Start Liftbridge server.
	server := createServer()
	require.NoError(t, server.Start())
	defer server.Stop()

	// Create partition where leader "a" is NOT in the ISR (simulating corrupt state).
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"b"}, // Leader "a" intentionally excluded
	}, false, nil)
	require.NoError(t, err)
	defer p.Close()

	// Verify leader is not in ISR initially.
	require.NotContains(t, p.GetISR(), "a")

	// Call becomeLeader - this should NOT panic and should add self to ISR.
	err = p.becomeLeader(1)
	require.NoError(t, err)

	// Verify leader was added to ISR.
	require.Contains(t, p.GetISR(), "a")
	require.Contains(t, p.GetISR(), "b")
}
