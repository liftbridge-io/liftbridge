package server

import (
	"testing"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	client "github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/require"

	"github.com/liftbridge-io/liftbridge/server/proto"
)

func createServer(leader bool) *Server {
	config := getTestConfig("a", leader, 0)
	config.Clustering.RaftBootstrapSeed = true
	return New(config)
}

func waitForCommitQueue(t *testing.T, timeout time.Duration, size int64, stream *stream) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if stream.commitQueue.Len() == size {
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
func TestStreamCommitLoopCommitNoAck(t *testing.T) {
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

	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject:  "foo",
		Name:     "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}))
	require.NoError(t, err)
	defer s.Close()
	s.commitQueue = queue.New(5)

	// Subscribe to ack inbox.
	ackInbox := "ack"
	sub, err := nc.SubscribeSync(ackInbox)
	require.NoError(t, err)
	nc.Flush()

	// Put some messages in the queue.
	s.commitQueue.Put(&client.Ack{Offset: 0, AckInbox: ackInbox})
	s.commitQueue.Put(&client.Ack{Offset: 1, AckInbox: ackInbox})
	s.commitQueue.Put(&client.Ack{Offset: 2, AckInbox: ackInbox})

	// Mark 0 and 1 as fully replicated.
	s.isr["a"].offset = 1
	s.isr["b"].offset = 2

	// Start commit loop.
	stop := make(chan struct{})
	go s.commitLoop(stop)

	// Trigger a commit.
	s.commitCheck <- struct{}{}

	// Wait for messages to be committed.
	waitForCommitQueue(t, 5*time.Second, 1, s)

	// Stop the loop.
	close(stop)

	// Verify HW.
	require.Equal(t, int64(1), s.log.HighWatermark())

	// Ensure no acks were published.
	_, err = sub.NextMsg(500 * time.Millisecond)
	require.Error(t, err)
}

// Ensure commitLoop commits messages in the queue when they have been
// replicated by all replicas in the ISR. This is done by removing committed
// messages and updating the HW. Ensure acks are sent when the AckPolicy is
// AckPolicy_ALL.
func TestStreamCommitLoopCommitAck(t *testing.T) {
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

	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject:  "foo",
		Name:     "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}))
	require.NoError(t, err)
	defer s.Close()
	s.commitQueue = queue.New(5)

	// Subscribe to ack inbox.
	ackInbox := "ack"
	sub, err := nc.SubscribeSync(ackInbox)
	require.NoError(t, err)
	nc.Flush()

	// Put some messages in the queue.
	s.commitQueue.Put(&client.Ack{Offset: 0, AckInbox: ackInbox, AckPolicy: client.AckPolicy_ALL})
	s.commitQueue.Put(&client.Ack{Offset: 1, AckInbox: ackInbox})
	s.commitQueue.Put(&client.Ack{Offset: 2, AckInbox: ackInbox, AckPolicy: client.AckPolicy_ALL})

	// Mark messages as fully replicated.
	s.isr["a"].offset = 2
	s.isr["b"].offset = 2

	// Start commit loop.
	stop := make(chan struct{})
	go s.commitLoop(stop)

	// Trigger a commit.
	s.commitCheck <- struct{}{}

	// Wait for messages to be committed.
	waitForCommitQueue(t, 5*time.Second, 0, s)

	// Stop the loop.
	close(stop)

	// Verify HW.
	require.Equal(t, int64(2), s.log.HighWatermark())

	// Ensure acks were published.
	_, err = sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	_, err = sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
}

// Ensure commitLoop is a no-op when the commitQueue is empty.
func TestStreamCommitLoopEmptyQueue(t *testing.T) {
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

	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject:  "foo",
		Name:     "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}))
	require.NoError(t, err)
	defer s.Close()
	s.commitQueue = queue.New(5)

	// Subscribe to ack inbox.
	ackInbox := "ack"
	sub, err := nc.SubscribeSync(ackInbox)
	require.NoError(t, err)
	nc.Flush()

	// Start commit loop.
	stop := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		s.commitLoop(stop)
		close(exited)
	}()

	// Trigger a commit.
	s.commitCheck <- struct{}{}

	// Stop the loop.
	close(stop)

	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected commit loop to exit")
	}

	// Verify HW.
	require.Equal(t, int64(-1), s.log.HighWatermark())

	// Ensure no acks were published.
	_, err = sub.NextMsg(500 * time.Millisecond)
	require.Error(t, err)
}

// Ensure commitLoop exits when the queue is disposed.
func TestStreamCommitLoopDisposedQueue(t *testing.T) {
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

	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject:  "foo",
		Name:     "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}))
	require.NoError(t, err)
	defer s.Close()
	s.commitQueue = queue.New(5)

	// Subscribe to ack inbox.
	ackInbox := "ack"
	sub, err := nc.SubscribeSync(ackInbox)
	require.NoError(t, err)
	nc.Flush()

	// Put some messages in the queue.
	s.commitQueue.Put(&client.Ack{Offset: 0, AckInbox: ackInbox})
	s.commitQueue.Put(&client.Ack{Offset: 1, AckInbox: ackInbox})
	s.commitQueue.Put(&client.Ack{Offset: 2, AckInbox: ackInbox})

	// Mark 0 and 1 as fully replicated.
	s.isr["a"].offset = 1
	s.isr["b"].offset = 2

	// Start commit loop.
	exited := make(chan struct{})
	go func() {
		s.commitLoop(make(chan struct{}))
		close(exited)
	}()

	// Dispose commit queue.
	s.commitQueue.Dispose()

	// Trigger a commit.
	s.commitCheck <- struct{}{}

	// Verify HW.
	require.Equal(t, int64(-1), s.log.HighWatermark())

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
func TestStreamCommitLoopNoCommitBelowMinISR(t *testing.T) {
	defer cleanupStorage(t)

	server := createServer(false)
	server.config.Clustering.MinISR = 2
	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject:  "foo",
		Name:     "foo",
		Replicas: []string{"a", "b"},
		Leader:   "a",
		Isr:      []string{"a", "b"},
	}))
	require.NoError(t, err)
	defer s.Close()
	s.commitQueue = queue.New(5)
	require.NoError(t, s.RemoveFromISR("b"))

	// Put some messages in the queue.
	s.commitQueue.Put(&client.Ack{Offset: 0})
	s.commitQueue.Put(&client.Ack{Offset: 1})
	s.commitQueue.Put(&client.Ack{Offset: 2})

	// Mark 0 and 1 as fully replicated.
	s.isr["a"].offset = 1

	// Start commit loop.
	stop := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		s.commitLoop(stop)
		close(exited)
	}()

	// Trigger a commit.
	s.commitCheck <- struct{}{}

	// Stop the loop.
	close(stop)

	// Ensure loop exited.
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected commit loop to exit")
	}

	// Verify nothing was committed.
	require.Equal(t, int64(3), s.commitQueue.Len())
	require.Equal(t, int64(-1), s.log.HighWatermark())
}

// Ensure RemoveFromISR returns an error if the replica is not a stream
// replica.
func TestStreamRemoveFromISRNotReplica(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer(false)
	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject: "foo",
		Name:    "foo",
	}))
	require.NoError(t, err)
	defer s.Close()
	require.Error(t, s.RemoveFromISR("foo"))
}

// Ensure RemoveFromISR removes the replica from the ISR and does not trigger a
// commit check on the follower.
func TestStreamRemoveFromISRFollower(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer(false)
	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject:  "foo",
		Name:     "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b", "c"},
	}))
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.RemoveFromISR("b"))

	// Verify there is no commit check.
	select {
	case <-s.commitCheck:
		t.Fatal("Unexpected commit check")
	default:
	}

	require.ElementsMatch(t, []string{"a", "c"}, s.GetISR())
	require.False(t, s.belowMinISR)
}

// Ensure RemoveFromISR removes the replica from the ISR and triggeris a commit
// check on the leader.
func TestStreamRemoveFromISRLeader(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer(false)
	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject:  "foo",
		Name:     "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "a",
		Isr:      []string{"a", "b", "c"},
	}))
	require.NoError(t, err)
	defer s.Close()
	s.isLeading = true
	require.NoError(t, s.RemoveFromISR("b"))

	// Verify there is a commit check.
	select {
	case <-s.commitCheck:
	default:
		t.Fatal("Did not get expected commit check")
	}

	require.ElementsMatch(t, []string{"a", "c"}, s.GetISR())
	require.False(t, s.belowMinISR)
}

// Ensure RemoveFromISR removes the replica from the ISR and marks the stream
// as below the minimum ISR when the ISR shrinks below the minimum.
func TestStreamRemoveFromISRBelowMin(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer(false)
	server.config.Clustering.MinISR = 3
	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject:  "foo",
		Name:     "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b", "c"},
	}))
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.RemoveFromISR("b"))

	require.ElementsMatch(t, []string{"a", "c"}, s.GetISR())
	require.True(t, s.belowMinISR)
}

// Ensure AddToISR returns an error if the replica is not a stream replica.
func TestStreamAddToISRNotReplica(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer(false)
	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject: "foo",
		Name:    "foo",
	}))
	require.NoError(t, err)
	defer s.Close()
	require.Error(t, s.AddToISR("foo"))
}

// Ensure AddToISR adds the replica to the ISR.
func TestStreamAddToISR(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer(false)
	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject:  "foo",
		Name:     "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b"},
	}))
	require.NoError(t, err)
	defer s.Close()

	require.ElementsMatch(t, []string{"a", "b"}, s.GetISR())

	require.NoError(t, s.AddToISR("c"))

	require.ElementsMatch(t, []string{"a", "b", "c"}, s.GetISR())
}

// Ensure AddToISR adds the replica to the ISR and, if the stream was below the
// minimum ISR and has recovered, marks the stream ISR as recovered.
func TestStreamAddToISRRecoverMin(t *testing.T) {
	defer cleanupStorage(t)
	server := createServer(false)
	server.config.Clustering.MinISR = 3
	s, err := server.newStream(proto.NewStreamWrapper(&proto.Stream{
		Subject:  "foo",
		Name:     "foo",
		Replicas: []string{"a", "b", "c"},
		Leader:   "b",
		Isr:      []string{"a", "b"},
	}))
	require.NoError(t, err)
	defer s.Close()
	s.belowMinISR = true

	require.ElementsMatch(t, []string{"a", "b"}, s.GetISR())

	require.NoError(t, s.AddToISR("c"))

	require.ElementsMatch(t, []string{"a", "b", "c"}, s.GetISR())
	require.False(t, s.belowMinISR)
}
