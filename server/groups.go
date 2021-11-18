package server

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

type partitionAssignments map[string][]int32

// consumer represents a member of a consumer group.
type consumer struct {
	id            string
	timer         *time.Timer
	streams       map[string]struct{}
	assignments   partitionAssignments
	assignedCount int
}

func (c *consumer) assignPartition(stream string, partition int32) {
	streamAssignments, ok := c.assignments[stream]
	if !ok {
		streamAssignments = []int32{}
	}
	c.assignments[stream] = append(streamAssignments, partition)
	c.assignedCount++
}

func (c *consumer) removeStreamAssignments(stream string) {
	c.assignedCount -= len(c.assignments[stream])
	delete(c.assignments, stream)
}

type consumerHeap []*consumer

func (c consumerHeap) Len() int { return len(c) }

func (c consumerHeap) Less(i, j int) bool {
	if c[i].assignedCount == c[j].assignedCount {
		// If the consumers have the same number of assignments, fall back to
		// lexicographical ordering of consumer ids to ensure a stable sort.
		return c[i].id < c[j].id
	}
	return c[i].assignedCount < c[j].assignedCount
}

func (c consumerHeap) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c *consumerHeap) Push(cons interface{}) {
	*c = append(*c, cons.(*consumer))
}

func (c *consumerHeap) Pop() interface{} {
	old := *c
	n := len(old)
	cons := old[n-1]
	old[n-1] = nil
	*c = old[0 : n-1]
	return cons
}

func (c consumerHeap) Peek() *consumer {
	return c[0]
}

// consumerGroup represents a group of consumers which consume a set of
// streams.
type consumerGroup struct {
	id               string
	members          map[string]*consumer
	subscribers      map[string]*consumerHeap // Maps streams to subscribed consumers
	coordinator      string
	coordinatorEpoch uint64 // Updates on coordinator changes
	membershipEpoch  uint64 // Updates on membership changes
	metadata         *metadataAPI
	recovered        bool
	mu               sync.RWMutex
}

func (m *metadataAPI) newConsumerGroup(protoGroup *proto.ConsumerGroup, recovered bool) (*consumerGroup, error) {
	group := &consumerGroup{
		id:          protoGroup.Id,
		members:     make(map[string]*consumer),
		subscribers: make(map[string]*consumerHeap),
		metadata:    m,
		recovered:   recovered,
	}
	if err := group.SetCoordinator(protoGroup.Coordinator, protoGroup.CoordinatorEpoch); err != nil {
		return nil, err
	}
	for _, member := range protoGroup.Members {
		if err := group.AddMember(member.Id, member.Streams); err != nil {
			group.Close()
			return nil, err
		}
	}
	group.mu.Lock()
	group.membershipEpoch = protoGroup.MembershipEpoch
	group.mu.Unlock()
	return group, nil
}

// String returns a human-readable representation of the consumer group.
func (c *consumerGroup) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return fmt.Sprintf("[id=%s, coordinator=%s, members=%d]", c.id, c.coordinator, len(c.members))
}

// GetID returns the group's ID.
func (c *consumerGroup) GetID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.id
}

// GetMembershipEpoch returns the group membership epoch which is updated on
// each membership change.
func (c *consumerGroup) GetMembershipEpoch() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.membershipEpoch
}

// SetCoordinator sets the coordinator for the group.
func (c *consumerGroup) SetCoordinator(coordinator string, coordinatorEpoch uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if coordinatorEpoch < c.coordinatorEpoch {
		return fmt.Errorf("proposed group coordinator epoch %d is less than current epoch %d",
			coordinatorEpoch, c.coordinatorEpoch)
	}
	c.coordinator = coordinator
	c.coordinatorEpoch = coordinatorEpoch
	return nil
}

// GetCoordinator returns the ID and epoch of the consumer group coordinator.
func (c *consumerGroup) GetCoordinator() (string, uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.coordinator, c.coordinatorEpoch
}

// StartRecovered starts the group if this server is the coordinator and the
// group is in recovery mode. This should be called for each consumer group
// after the recovery process completes. Returns a bool indicating if the group
// was recovered.
func (c *consumerGroup) StartRecovered() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.recovered {
		return false
	}
	if c.coordinator == c.metadata.config.Clustering.ServerID {
		for memberID, member := range c.members {
			// If for some reason the member already has a timer, cancel it.
			if member.timer != nil {
				member.timer.Stop()
			}
			member.timer = c.startMemberTimer(memberID)
		}
	}
	c.recovered = false
	return true
}

// IsMember indicates if the given consumer is a member of the group.
func (c *consumerGroup) IsMember(consumerID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.members[consumerID]
	return ok
}

// GetMembers returns a map of the group members to their subscribed streams.
func (c *consumerGroup) GetMembers() map[string][]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	members := make(map[string][]string, len(c.members))
	for id, member := range c.members {
		streams := make([]string, 0, len(member.streams))
		for stream := range member.streams {
			streams = append(streams, stream)
		}
		members[id] = streams
	}
	return members
}

// AddMember adds the given consumer to the group. If this server is the group
// coordinator, this will start a timer to ensure liveness of the consumer
// unless the group is in recovery mode.
func (c *consumerGroup) AddMember(consumerID string, streams []string) error {
	// Verify the streams exist.
	for _, stream := range streams {
		if c.metadata.GetStream(stream) == nil {
			return fmt.Errorf("stream %s does not exist", stream)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.members[consumerID]; ok {
		return ErrConsumerAlreadyMember
	}
	var timer *time.Timer

	// If this group is not in recovery mode and this server is the
	// coordinator, start a liveness timer for the consumer.
	if !c.recovered && c.coordinator == c.metadata.config.Clustering.ServerID {
		timer = c.startMemberTimer(consumerID)
	}

	streamSet := make(map[string]struct{}, len(streams))
	for _, stream := range streams {
		streamSet[stream] = struct{}{}
	}
	cons := &consumer{
		id:          consumerID,
		timer:       timer,
		streams:     streamSet,
		assignments: make(partitionAssignments),
	}
	c.members[consumerID] = cons
	// Assign partitions if this server is the coordinator.
	if c.coordinator == c.metadata.config.Clustering.ServerID {
		c.addConsumer(cons)
	}
	c.membershipEpoch++
	return nil
}

// RemoveMember removes the given consumer from the group. If this server is
// the group coordinator, this will stop the liveness timer for the consumer.
// Returns a bool indicating if this was the last member of the group.
func (c *consumerGroup) RemoveMember(consumerID string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	consumer, ok := c.members[consumerID]
	if !ok {
		return false, ErrConsumerNotMember
	}
	if consumer.timer != nil {
		consumer.timer.Stop()
	}
	// Balance assignments if this server is the coordinator.
	if c.coordinator == c.metadata.config.Clustering.ServerID {
		c.removeConsumer(consumer)
	}
	delete(c.members, consumerID)
	c.membershipEpoch++
	return len(c.members) == 0, nil
}

// StreamDeleted is called whenever a stream is deleted.
func (c *consumerGroup) StreamDeleted(stream string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	subscribers, ok := c.subscribers[stream]
	if !ok {
		return
	}
	rebalance := make(map[string]struct{})
	for _, subscriber := range *subscribers {
		delete(subscriber.streams, stream)
		subscriber.removeStreamAssignments(stream)
		for subscribedTo := range subscriber.streams {
			rebalance[subscribedTo] = struct{}{}
		}
	}
	delete(c.subscribers, stream)
	// Rebalance assignments for all other streams the affected consumers are
	// subscribed to.
	for stream := range rebalance {
		// Enforce heap invariants.
		subscribers, ok := c.subscribers[stream]
		if ok {
			heap.Init(subscribers)
		}
		c.balanceAssignmentsForStream(stream)
	}
}

// GetAssignments returns the partition assignments for the given consumer
// along with the group epoch. It returns an error if the consumer is not a
// member of the group, if this server is not the group coordinator, or the
// provided coordinator epoch is greater than the current known epoch.
func (c *consumerGroup) GetAssignments(consumerID string, coordinatorEpoch uint64) (
	partitionAssignments, uint64, error) {

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.coordinator != c.metadata.config.Clustering.ServerID {
		return nil, 0, ErrBrokerNotCoordinator
	}

	if coordinatorEpoch > c.coordinatorEpoch {
		return nil, 0, ErrCoordinatorEpoch
	}

	member, ok := c.members[consumerID]
	if !ok {
		return nil, 0, ErrConsumerNotMember
	}

	// Reset liveness timer.
	if member.timer == nil {
		// This shouldn't happen.
		return nil, 0, errors.New("consumer not active for server (no timer)")
	}
	member.timer.Reset(c.metadata.config.Consumers.Timeout)

	return member.assignments, c.membershipEpoch, nil
}

// Close should be called when the consumer group is being deleted.
func (c *consumerGroup) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for member, consumer := range c.members {
		if consumer.timer != nil {
			consumer.timer.Stop()
		}
		delete(c.members, member)
	}
}

// startMemberTimer starts a liveness timer for the given member.
func (c *consumerGroup) startMemberTimer(consumerID string) *time.Timer {
	return time.AfterFunc(c.metadata.config.Consumers.Timeout, c.consumerExpired(consumerID))
}

// consumerExpired returns a callback function which is invoked when a consumer
// times out. This will attempt to remove the expired consumer from the group.
func (c *consumerGroup) consumerExpired(consumerID string) func() {
	return func() {
		c.metadata.logger.Errorf("Consumer %s timed out for consumer group %s, removing from group",
			consumerID, c.id)
		if err := c.removeExpiredMember(consumerID); err != nil {
			c.metadata.logger.Errorf("Failed to remove consumer %s from consumer group %s: %v",
				consumerID, c.id, err.Error())
			// Reset the timer so we can try again later.
			timer := c.startMemberTimer(consumerID)
			c.mu.Lock()
			consumer := c.members[consumerID]
			consumer.timer = timer
			c.mu.Unlock()
		}
	}
}

// removeExpiredMember sends a LeaveConsumerGroup request to the controller to
// remove the expired consumer from the group.
func (c *consumerGroup) removeExpiredMember(consumerID string) error {
	req := &proto.LeaveConsumerGroupOp{
		GroupId:    c.id,
		ConsumerId: consumerID,
	}
	if err := c.metadata.LeaveConsumerGroup(context.Background(), req); err != nil {
		return err.Err()
	}
	return nil
}

// addConsumer adds the given consumer to the group's subscriber heaps for
// consumer's streams. This will result in balancing the partition assignments
// for these streams. This must be called within the group mutex.
func (c *consumerGroup) addConsumer(cons *consumer) {
	for stream := range cons.streams {
		subscribers, ok := c.subscribers[stream]
		if !ok {
			subscribers = &consumerHeap{}
		}
		heap.Push(subscribers, cons)
		c.balanceAssignmentsForStream(stream)
	}
}

// removeConsumer removes the given consumer from the group's subscriber heaps
// for consumer's streams. This will result in balancing the partition
// assignments for any streams this consumer had assignments for. This must be
// called within the group mutex.
func (c *consumerGroup) removeConsumer(cons *consumer) {
	for stream := range cons.streams {
		subscribers, ok := c.subscribers[stream]
		if !ok {
			continue
		}
		for i, sub := range *subscribers {
			if cons == sub {
				heap.Remove(subscribers, i)
			}
		}
		// Rebalance the stream if the consumer being removed had assignments
		// for it.
		if _, ok := cons.assignments[stream]; ok {
			c.balanceAssignmentsForStream(stream)
		}
	}
}

// balanceAssignmentsForStream assigns the partitions for the given stream to
// interested consumers in the group. This attempts to distribute assignments
// by assigning each partition to the next consumer with the least amount of
// assignments. This must be called within the group mutex.
func (c *consumerGroup) balanceAssignmentsForStream(streamName string) {
	subscribers, ok := c.subscribers[streamName]
	if !ok || len(*subscribers) == 0 {
		return
	}
	stream := c.metadata.GetStream(streamName)
	if stream == nil {
		return
	}

	// Reset assignments for stream.
	// TODO: This rebalancing could probably be implemented in a more optimized
	// way, e.g. avoiding unnecessary reassignments of partitions. For
	// instance, if there is excess capacity (more consumers than partitions),
	// there is no reason to reassign partitions.
	for _, subscriber := range *subscribers {
		subscriber.removeStreamAssignments(streamName)
	}
	heap.Init(subscribers)

	// Put partitions to assign in a sorted list first in order to minimize
	// unnecessary reassignments.
	var (
		partitionsMap = stream.GetPartitions()
		partitions    = make([]int32, len(partitionsMap))
		i             = 0
	)
	for partition := range partitionsMap {
		partitions[i] = partition
		i++
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	// Assign each partition to the consumer with the least amount of
	// assignments.
	for _, partition := range partitions {
		minConsumer := subscribers.Peek()
		c.assignPartition(streamName, partition, minConsumer)
	}
}

func (c *consumerGroup) assignPartition(stream string, partition int32, subscriber *consumer) {
	subscriber.assignPartition(stream, partition)
	// Enforce heap invariants.
	for stream := range subscriber.streams {
		subscribers, ok := c.subscribers[stream]
		if ok {
			heap.Init(subscribers)
		}
	}
}
