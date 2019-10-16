package server

import (
	"testing"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	client "github.com/liftbridge-io/liftbridge-grpc/go"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/liftbridge-io/liftbridge/server/proto"
)

func createServer(leader bool) *Server {
	config := getTestConfig("a", leader, 0)
	config.Clustering.RaftBootstrapSeed = true
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

// Ensure commitLoop commits messages in the queue when they have been
// replicated by all replicas in the ISR. This is done by removing committed
// messages and updating the HW. Ensure acks are not sent when the AckPolicy is
// not AckPolicy_ALL.
func TestPartitionCommitLoopCommitNoAck(t *testing.T) {
	defer cleanupStorage(t)

	// Start NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Start Liftbridge server.
	server := createServer(false)
	require.NoError(t, server.Start())
	defer server.Stop()

	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}, false)
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

	// Start NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Start Liftbridge server.
	server := createServer(false)
	require.NoError(t, server.Start())
	defer server.Stop()

	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}, false)
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

	// Start NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Start Liftbridge server.
	server := createServer(false)
	require.NoError(t, server.Start())
	defer server.Stop()

	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}, false)
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

	// Start NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Start Liftbridge server.
	server := createServer(false)
	require.NoError(t, server.Start())
	defer server.Stop()

	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}, false)
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

	server := createServer(false)
	server.config.Clustering.MinISR = 2
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}, false)
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
	server := createServer(false)
	p, err := server.newPartition(&proto.Partition{
		Subject: "foo",
		Stream:  "foo",
	}, false)
	require.NoError(t, err)
	defer p.Close()
	require.Error(t, p.RemoveFromISR("foo"))
}

// Ensure RemoveFromISR removes the replica from the ISR and does not trigger a
// commit check on the follower.
func TestPartitionRemoveFromISRFollower(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer(false)
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b", "c"},
	}, false)
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
	server := createServer(false)
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "a",
		Isr:      []string{"a", "b", "c"},
	}, false)
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
	server := createServer(false)
	server.config.Clustering.MinISR = 3
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b", "c"},
	}, false)
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.RemoveFromISR("b"))

	require.ElementsMatch(t, []string{"a", "c"}, p.GetISR())
	require.True(t, p.belowMinISR)
}

// Ensure AddToISR returns an error if the replica is not a stream replica.
func TestPartitionAddToISRNotReplica(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer(false)
	p, err := server.newPartition(&proto.Partition{
		Subject: "foo",
		Stream:  "foo",
	}, false)
	require.NoError(t, err)
	defer p.Close()
	require.Error(t, p.AddToISR("foo"))
}

// Ensure AddToISR adds the replica to the ISR.
func TestPartitionAddToISR(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer(false)
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b"},
	}, false)
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
	server := createServer(false)
	server.config.Clustering.MinISR = 3
	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b"},
	}, false)
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

	// Start NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Start Liftbridge server.
	config := getTestConfig("a", true, 5050)
	// Set idle wait long enough that it shouldn't be reached.
	config.Clustering.ReplicaMaxIdleWait = time.Hour
	server := runServerWithConfig(t, config)
	defer server.Stop()

	p, err := server.newPartition(&proto.Partition{
		Subject:  "foo",
		Stream:   "foo",
		Replicas: []string{"a", "b"},
		Leader:   "b",
		Isr:      []string{"a", "b"},
	}, false)
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
