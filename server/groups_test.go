package server

import (
	"container/heap"
	"context"
	"strconv"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
	proto "github.com/liftbridge-io/liftbridge/server/protocol"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitForGroup(t *testing.T, timeout time.Duration, id string, servers ...*Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			group := s.metadata.GetConsumerGroup(id)
			if group == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
		}
		return
	}
	stackFatalf(t, "Cluster did not create group [id=%s]", id)
}

func waitForConsumer(t *testing.T, timeout time.Duration, group, cons string, servers ...*Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			group := s.metadata.GetConsumerGroup(group)
			if group == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
			if !group.IsMember(cons) {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
		}
		return
	}
	stackFatalf(t, "Group [id=%s] does not have consumer [id=%s]", group, cons)
}

func waitForGroupMembers(t *testing.T, timeout time.Duration, group string, members int, servers ...*Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			group := s.metadata.GetConsumerGroup(group)
			if group == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
			if len(group.GetMembers()) != members {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
		}
		return
	}
	stackFatalf(t, "Group [id=%s] does not have %d members", group, members)
}

func waitForConsumerSubscribed(t *testing.T, timeout time.Duration, group, cons, stream string,
	partition int32, server *Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		partition := server.metadata.GetPartition(stream, partition)
		if partition == nil {
			time.Sleep(15 * time.Millisecond)
			continue LOOP
		}
		member := partition.GetGroupConsumer(group)
		if member == nil {
			time.Sleep(15 * time.Millisecond)
			continue LOOP
		}
		if member.consumerID != cons {
			time.Sleep(15 * time.Millisecond)
			continue LOOP
		}
		return
	}
	stackFatalf(t,
		"Group [id=%s] does not have consumer [id=%s] subscribed to [stream=%s, partition=%d]",
		group, cons, stream, partition)
}

func getGroupCoordinator(t *testing.T, timeout time.Duration, groupName string, servers ...*Server) (
	*Server, uint64) {
	var (
		epoch       uint64
		coordinator string
		deadline    = time.Now().Add(timeout)
	)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			group := s.metadata.GetConsumerGroup(groupName)
			if group == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
			coor, ep := group.GetCoordinator()
			if ep >= epoch {
				coordinator = coor
				epoch = ep
			}
		}
		if coordinator == "" {
			time.Sleep(15 * time.Millisecond)
			continue LOOP
		}

		for _, s := range servers {
			if s.config.Clustering.ServerID == coordinator {
				return s, epoch
			}
		}
	}

	stackFatalf(t, "No coordinator found for group [id=%s]", groupName)
	return nil, 0
}

// Ensure assignPartition and removeStreamAssignments update the consumer's
// assignments and assignedCount appropriately.
func TestConsumerGroupAssignments(t *testing.T) {
	c := &consumer{assignments: make(partitionAssignments)}

	assert.Equal(t, 0, c.assignedCount)
	assert.Len(t, c.assignments, 0)

	c.assignPartition("foo", 0)
	c.assignPartition("foo", 1)
	c.assignPartition("foo", 2)
	c.assignPartition("bar", 0)

	assert.Equal(t, 4, c.assignedCount)
	assert.Len(t, c.assignments, 2)
	assert.Len(t, c.assignments["foo"], 3)
	assert.Len(t, c.assignments["bar"], 1)

	c.removeStreamAssignments("foo")

	assert.Len(t, c.assignments, 1)
	assert.Len(t, c.assignments["bar"], 1)
	assert.Equal(t, 1, c.assignedCount)

	c.removeStreamAssignments("bar")

	assert.Equal(t, 0, c.assignedCount)
	assert.Len(t, c.assignments, 0)
}

// Ensure consumerHeap implements the heap interface correctly ordering by
// assignment count.
func TestConsumerGroupHeap(t *testing.T) {
	h := new(consumerHeap)
	heap.Push(h, &consumer{id: "a", assignedCount: 5})
	heap.Push(h, &consumer{id: "b", assignedCount: 2})
	heap.Push(h, &consumer{id: "c", assignedCount: 10})
	heap.Push(h, &consumer{id: "d", assignedCount: 3})
	heap.Push(h, &consumer{id: "e", assignedCount: 0})
	heap.Push(h, &consumer{id: "f", assignedCount: 3})

	assert.Equal(t, 6, h.Len())

	p := h.Peek()
	assert.Equal(t, "e", p.id)

	c := heap.Pop(h).(*consumer)
	assert.Equal(t, p, c)
	assert.Equal(t, 5, h.Len())

	heap.Push(h, &consumer{id: "g", assignedCount: 1})
	assert.Equal(t, 6, h.Len())

	c = heap.Pop(h).(*consumer)
	assert.Equal(t, "g", c.id)
	assert.Equal(t, 5, h.Len())

	c = heap.Pop(h).(*consumer)
	assert.Equal(t, "b", c.id)
	assert.Equal(t, 4, h.Len())

	c = heap.Pop(h).(*consumer)
	assert.Equal(t, "d", c.id)
	assert.Equal(t, 3, h.Len())

	c = heap.Pop(h).(*consumer)
	assert.Equal(t, "f", c.id)
	assert.Equal(t, 2, h.Len())

	c = heap.Pop(h).(*consumer)
	assert.Equal(t, "a", c.id)
	assert.Equal(t, 1, h.Len())

	c = heap.Pop(h).(*consumer)
	assert.Equal(t, "c", c.id)
	assert.Equal(t, 0, h.Len())
}

// Ensure rangeStreamsOrdered applies the supplied function to the map in
// order.
func TestConsumerGroupRangeStreamsOrdered(t *testing.T) {
	streams := map[string]struct{}{
		"b": {},
		"d": {},
		"a": {},
		"c": {},
		"e": {},
	}
	applied := []string{}
	rangeStreamsOrdered(streams, func(stream string) {
		applied = append(applied, stream)
	})
	assert.Len(t, applied, 5)
	assert.Equal(t, "a", applied[0])
	assert.Equal(t, "b", applied[1])
	assert.Equal(t, "c", applied[2])
	assert.Equal(t, "d", applied[3])
	assert.Equal(t, "e", applied[4])
}

// Ensure consumer groups are created, assign partitions, and are deleted
// correctly.
func TestConsumerGroupLifecycle(t *testing.T) {
	defer cleanupStorage(t)

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.CursorsStream.Partitions = 1
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create streams.
	err = client.CreateStream(context.Background(), "foo", "foo", lift.Partitions(2))
	require.NoError(t, err)

	err = client.CreateStream(context.Background(), "bar", "bar", lift.Partitions(2))
	require.NoError(t, err)

	var (
		group   = "my-group"
		cons1ID = "consumer-1"
		cons2ID = "consumer-2"
	)

	// Create consumer.
	cons1, err := client.CreateConsumer(group, lift.ConsumerID(cons1ID),
		lift.FetchAssignmentsInterval(func(timeout time.Duration) time.Duration {
			return 50 * time.Millisecond
		}))
	require.NoError(t, err)

	var (
		cons1FooPartition0 = make(chan *lift.Message)
		cons1FooPartition1 = make(chan *lift.Message)
		cons1BarPartition0 = make(chan *lift.Message)
		cons1BarPartition1 = make(chan *lift.Message)
	)

	err = cons1.Subscribe(context.Background(), []string{"foo", "bar"},
		func(msg *lift.Message, err error) {
			if err != nil {
				return
			}
			if msg.Stream() == "foo" {
				if msg.Partition() == 0 {
					cons1FooPartition0 <- msg
				} else {
					cons1FooPartition1 <- msg
				}
			} else {
				if msg.Partition() == 0 {
					cons1BarPartition0 <- msg
				} else {
					cons1BarPartition1 <- msg
				}
			}
		})
	require.NoError(t, err)

	waitForGroup(t, 5*time.Second, group, s1)
	waitForConsumer(t, 5*time.Second, group, cons1ID, s1)
	waitForConsumerSubscribed(t, 5*time.Second, group, cons1ID, "foo", 0, s1)
	waitForConsumerSubscribed(t, 5*time.Second, group, cons1ID, "foo", 1, s1)
	waitForConsumerSubscribed(t, 5*time.Second, group, cons1ID, "bar", 0, s1)
	waitForConsumerSubscribed(t, 5*time.Second, group, cons1ID, "bar", 1, s1)

	// Publish some messages and validate we receive them.
	for _, stream := range []string{"foo", "bar"} {
		for i := 0; i < 2; i++ {
			for j := 0; j < 3; j++ {
				_, err = client.Publish(context.Background(), stream, []byte(strconv.Itoa(j)),
					lift.ToPartition(int32(i)))
				require.NoError(t, err)
			}
		}
	}

	for partition, ch := range []chan *lift.Message{cons1FooPartition0, cons1FooPartition1} {
		for i := 0; i < 3; i++ {
			select {
			case msg := <-ch:
				require.Equal(t, "foo", msg.Stream())
				require.Equal(t, int32(partition), msg.Partition())
				require.Equal(t, int64(i), msg.Offset())
				require.Equal(t, []byte(strconv.Itoa(i)), msg.Value())
			case <-time.After(5 * time.Second):
				t.Fatal("Did not receive expected message")
			}
		}
	}
	for partition, ch := range []chan *lift.Message{cons1BarPartition0, cons1BarPartition1} {
		for i := 0; i < 3; i++ {
			select {
			case msg := <-ch:
				require.Equal(t, "bar", msg.Stream())
				require.Equal(t, int32(partition), msg.Partition())
				require.Equal(t, int64(i), msg.Offset())
				require.Equal(t, []byte(strconv.Itoa(i)), msg.Value())
			case <-time.After(5 * time.Second):
				t.Fatal("Did not receive expected message")
			}
		}
	}

	// Create another consumer.
	cons2, err := client.CreateConsumer(group, lift.ConsumerID(cons2ID))
	require.NoError(t, err)

	var (
		cons2FooPartition0 = make(chan *lift.Message)
		cons2FooPartition1 = make(chan *lift.Message)
	)

	// Consumer 2 should take over subscriptions for stream foo.
	err = cons2.Subscribe(context.Background(), []string{"foo"},
		func(msg *lift.Message, err error) {
			if err != nil {
				return
			}
			require.Equal(t, "foo", msg.Stream())
			if msg.Partition() == 0 {
				cons2FooPartition0 <- msg
			} else {
				cons2FooPartition1 <- msg
			}
		})
	require.NoError(t, err)

	waitForConsumer(t, 5*time.Second, group, cons2ID, s1)
	waitForConsumerSubscribed(t, 5*time.Second, group, cons2ID, "foo", 0, s1)
	waitForConsumerSubscribed(t, 5*time.Second, group, cons2ID, "foo", 1, s1)

	// Publish some more messages.
	for _, stream := range []string{"foo", "bar"} {
		for i := 0; i < 2; i++ {
			for j := 0; j < 3; j++ {
				_, err = client.Publish(context.Background(), stream, []byte(strconv.Itoa(j)),
					lift.ToPartition(int32(i)))
				require.NoError(t, err)
			}
		}
	}

	// Ensure consumer 2 received messages on foo.
	for partition, ch := range []chan *lift.Message{cons2FooPartition0, cons2FooPartition1} {
		for i := 0; i < 3; i++ {
			select {
			case msg := <-ch:
				require.Equal(t, "foo", msg.Stream())
				require.Equal(t, int32(partition), msg.Partition())
				require.Equal(t, int64(i+3), msg.Offset())
				require.Equal(t, []byte(strconv.Itoa(i)), msg.Value())
			case <-time.After(5 * time.Second):
				t.Fatal("Did not receive expected message")
			}
		}
	}

	// Ensure consumer 1 did not receive messages on foo.
	for _, ch := range []chan *lift.Message{cons1FooPartition0, cons1FooPartition1} {
		select {
		case <-ch:
			t.Fatal("Received unexpected message")
		default:
		}
	}

	// Ensure consumer 1 received messages on bar.
	for partition, ch := range []chan *lift.Message{cons1BarPartition0, cons1BarPartition1} {
		for i := 0; i < 3; i++ {
			select {
			case msg := <-ch:
				require.Equal(t, "bar", msg.Stream())
				require.Equal(t, int32(partition), msg.Partition())
				require.Equal(t, int64(i+3), msg.Offset())
				require.Equal(t, []byte(strconv.Itoa(i)), msg.Value())
			case <-time.After(5 * time.Second):
				t.Fatal("Did not receive expected message")
			}
		}
	}

	// Remove consumer 2 from group and validate consumer 1's assignments are
	// updated.
	require.NoError(t, cons2.Close())
	waitForConsumerSubscribed(t, 5*time.Second, group, cons1ID, "foo", 0, s1)
	waitForConsumerSubscribed(t, 5*time.Second, group, cons1ID, "foo", 1, s1)
	waitForConsumerSubscribed(t, 5*time.Second, group, cons1ID, "bar", 0, s1)
	waitForConsumerSubscribed(t, 5*time.Second, group, cons1ID, "bar", 1, s1)

	// Remove consumer 1 from group. This should remove the group since it's
	// the last member.
	require.NoError(t, cons1.Close())
	require.Nil(t, s1.metadata.GetConsumerGroup(group))
}

// Ensure joining a consumer group fails if the consumer subscribes to a stream
// that doesn't exist or if a consumer with the same ID is already a member.
func TestConsumerGroupAddMember(t *testing.T) {
	defer cleanupStorage(t)

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.CursorsStream.Partitions = 1
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	var (
		group      = "my-group"
		consumerID = "cons"
	)

	// Create consumer.
	cons, err := client.CreateConsumer(group, lift.ConsumerID(consumerID))
	require.NoError(t, err)

	// This should error because stream foo does not exist.
	err = cons.Subscribe(context.Background(), []string{"foo"}, func(msg *lift.Message, err error) {
	})
	require.Error(t, err)

	// Create stream.
	err = client.CreateStream(context.Background(), "foo", "foo")
	require.NoError(t, err)

	// This should now succeed.
	err = cons.Subscribe(context.Background(), []string{"foo"}, func(msg *lift.Message, err error) {
	})
	require.NoError(t, err)

	// Create consumer with same ID.
	cons2, err := client.CreateConsumer(group, lift.ConsumerID(consumerID))
	require.NoError(t, err)

	// This should error because a consumer with the same ID is already a
	// member.
	err = cons2.Subscribe(context.Background(), []string{"foo"}, func(msg *lift.Message, err error) {
	})
	require.Error(t, err)
}

// Ensure when a stream that a consumer is subscribed to is deleted, the stream
// is removed from the consumer.
func TestConsumerGroupStreamDeleted(t *testing.T) {
	defer cleanupStorage(t)

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.CursorsStream.Partitions = 1
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	var (
		group       = "my-group"
		consumerID1 = "cons1"
	)

	// Create streams.
	err = client.CreateStream(context.Background(), "foo", "foo")
	require.NoError(t, err)
	err = client.CreateStream(context.Background(), "bar", "bar")
	require.NoError(t, err)

	// Create consumer.
	cons1, err := client.CreateConsumer(group, lift.ConsumerID(consumerID1))
	require.NoError(t, err)

	err = cons1.Subscribe(context.Background(), []string{"foo", "bar"},
		func(msg *lift.Message, err error) {
		})
	require.NoError(t, err)

	g := s1.metadata.GetConsumerGroup(group)
	require.NotNil(t, g)
	members := g.GetMembers()
	require.Len(t, members, 1)
	streams := members[consumerID1]
	require.Len(t, streams, 2)
	require.Contains(t, streams, "foo")
	require.Contains(t, streams, "bar")

	client.DeleteStream(context.Background(), "foo")
	members = g.GetMembers()
	require.Len(t, members, 1)
	streams = members[consumerID1]
	require.Len(t, streams, 1)
	require.Equal(t, "bar", streams[0])
}

// Ensure when a group coordinator becomes unavailable, the consumer reports it
// and it fails over to another server.
func TestConsumerGroupCoordinatorFailover(t *testing.T) {
	defer cleanupStorage(t)

	// Use an external NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.EmbeddedNATS = false
	s1Config.CursorsStream.Partitions = 1
	s1Config.Groups.CoordinatorTimeout = 50 * time.Millisecond
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2Config.CursorsStream.Partitions = 1
	s2Config.Groups.CoordinatorTimeout = 50 * time.Millisecond
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Configure third server.
	s3Config := getTestConfig("c", false, 5052)
	s3Config.CursorsStream.Partitions = 1
	s3Config.Groups.CoordinatorTimeout = 50 * time.Millisecond
	s3 := runServerWithConfig(t, s3Config)
	defer s3.Stop()

	getMetadataLeader(t, 10*time.Second, s1, s2, s3)

	client, err := lift.Connect([]string{"localhost:5050", "localhost:5051", "localhost:5052"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	err = client.CreateStream(context.Background(), "foo", "foo")
	require.NoError(t, err)

	var (
		group  = "my-group"
		consID = "cons"
	)

	// Create consumer.
	cons, err := client.CreateConsumer(group, lift.ConsumerID(consID),
		lift.FetchAssignmentsInterval(func(timeout time.Duration) time.Duration {
			return 10 * time.Millisecond
		}))
	require.NoError(t, err)

	err = cons.Subscribe(context.Background(), []string{"foo"},
		func(msg *lift.Message, err error) {
		})
	require.NoError(t, err)

	waitForConsumer(t, 5*time.Second, group, consID, s1, s2, s3)
	coordinator, epoch := getGroupCoordinator(t, 5*time.Second, group, s1, s2, s3)

	// Stop coordinator to force a failover.
	require.NoError(t, coordinator.Stop())
	var followers []*Server
	if coordinator == s1 {
		followers = []*Server{s2, s3}
	} else if coordinator == s2 {
		followers = []*Server{s1, s3}
	} else {
		followers = []*Server{s1, s2}
	}

	// Wait for new coordinator.
	var (
		deadline       = time.Now().Add(5 * time.Second)
		newCoordinator *Server
		newEpoch       uint64
	)
	for time.Now().Before(deadline) {
		newCoordinator, newEpoch = getGroupCoordinator(t, 10*time.Second, group, followers...)
		if newCoordinator == coordinator {
			time.Sleep(15 * time.Millisecond)
			continue
		}
		break
	}

	require.NotEqual(t, newCoordinator, coordinator)
	require.Greater(t, newEpoch, epoch)
}

// Ensure when a consumer's timeout is reached the group invokes the expired
// handler.
func TestConsumerGroupConsumerTimeout(t *testing.T) {
	protoGroup := &proto.ConsumerGroup{
		Id:          "my-group",
		Coordinator: "a",
	}
	called := 0
	ch := make(chan struct{}, 1)
	handler := func(groupID, consumerID string) error {
		require.Equal(t, "my-group", groupID)
		require.Equal(t, "cons1", consumerID)
		if called == 0 {
			called++
			return errors.New("error")
		}
		called++
		select {
		case ch <- struct{}{}:
		default:
		}
		return nil
	}

	getPartitions := func(stream string) int32 {
		return 1
	}

	group := newConsumerGroup("a", time.Millisecond, protoGroup, false,
		noopLogger(), handler, getPartitions)
	defer group.Close()

	require.NoError(t, group.AddMember("cons1", []string{"foo"}, 1))

	select {
	case <-ch:
		require.Equal(t, 2, called)
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called")
	}
}

// Ensure when StartRecovered is called on a group that is not being recovered
// it returns false. When the group is being recovered and it's the
// coordinator, member timers are started and true is returned.
func TestConsumerGroupStartRecovered(t *testing.T) {
	protoGroup := &proto.ConsumerGroup{
		Id:          "my-group",
		Coordinator: "a",
		Members: []*proto.Consumer{
			&proto.Consumer{
				Id:      "cons1",
				Streams: []string{"foo"},
			},
		},
	}
	called := 0
	ch := make(chan struct{}, 1)
	handler := func(groupID, consumerID string) error {
		require.Equal(t, "my-group", groupID)
		require.Equal(t, "cons1", consumerID)
		called++
		select {
		case ch <- struct{}{}:
		default:
		}
		return nil
	}

	getPartitions := func(stream string) int32 {
		return 1
	}

	group := newConsumerGroup("a", time.Minute, protoGroup, false,
		noopLogger(), handler, getPartitions)
	defer group.Close()

	// Group not in recovery should return false.
	require.False(t, group.StartRecovered())

	group = newConsumerGroup("a", time.Millisecond, protoGroup, true,
		noopLogger(), handler, getPartitions)
	defer group.Close()

	// Group in recovery should return true.
	require.True(t, group.StartRecovered())

	select {
	case <-ch:
		require.Equal(t, 1, called)
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called")
	}

	require.False(t, group.recovered)
}

// Ensure SetCoordinator returns an error if the provided epoch is less than
// the current epoch. Otherwise it updates the coordinator and epoch and, if
// the new coordinator is the current server, starts member timers.
func TestConsumerGroupSetCoordinator(t *testing.T) {
	protoGroup := &proto.ConsumerGroup{
		Id:          "my-group",
		Coordinator: "a",
		Epoch:       1,
		Members: []*proto.Consumer{
			&proto.Consumer{
				Id:      "cons1",
				Streams: []string{"foo"},
			},
		},
	}
	called := 0
	ch := make(chan struct{}, 1)
	handler := func(groupID, consumerID string) error {
		require.Equal(t, "my-group", groupID)
		require.Equal(t, "cons1", consumerID)
		called++
		select {
		case ch <- struct{}{}:
		default:
		}
		return nil
	}

	getPartitions := func(stream string) int32 {
		return 1
	}

	group := newConsumerGroup("a", time.Millisecond, protoGroup, false,
		noopLogger(), handler, getPartitions)
	defer group.Close()

	// SetCoordinator with an epoch less than current epoch should return
	// an error.
	require.Error(t, group.SetCoordinator("b", 0))

	require.NoError(t, group.SetCoordinator("b", 2))

	coordinator, epoch := group.GetCoordinator()
	require.Equal(t, "b", coordinator)
	require.Equal(t, uint64(2), epoch)

	// Changing to this server should start timers.
	require.NoError(t, group.SetCoordinator("a", 3))

	select {
	case <-ch:
		require.Equal(t, 1, called)
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called")
	}
}

// Ensure GetID returns the group ID.
func TestConsumerGroupGetID(t *testing.T) {
	protoGroup := &proto.ConsumerGroup{
		Id:          "my-group",
		Coordinator: "a",
	}

	group := newConsumerGroup("a", time.Millisecond, protoGroup, false,
		noopLogger(), nil, nil)
	defer group.Close()

	require.Equal(t, "my-group", group.GetID())
}
