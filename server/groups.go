package server

import (
	"container/heap"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize/english"

	"github.com/liftbridge-io/liftbridge/server/logger"
	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

type partitionAssignments map[string][]int32

type groupMemberExpiredHandler func(groupID, consumerID string) error

type getStreamPartitions func(stream string) int32

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
	serverID             string
	id                   string
	members              map[string]*consumer
	subscribers          map[string]*consumerHeap // Maps streams to subscribed consumers
	consumerTimeout      time.Duration
	coordinator          string
	epoch                uint64 // Updates on coordinator and assignment changes
	getStreamPartitions  getStreamPartitions
	memberExpiredHandler groupMemberExpiredHandler
	recovered            bool
	mu                   sync.RWMutex
	logger               logger.Logger
}

func newConsumerGroup(serverID string, consumerTimeout time.Duration, protoGroup *proto.ConsumerGroup,
	recovered bool, logger logger.Logger, memberExpiredHandler groupMemberExpiredHandler,
	getPartitions getStreamPartitions) *consumerGroup {

	group := &consumerGroup{
		serverID:             serverID,
		id:                   protoGroup.Id,
		members:              make(map[string]*consumer),
		subscribers:          make(map[string]*consumerHeap),
		getStreamPartitions:  getPartitions,
		consumerTimeout:      consumerTimeout,
		memberExpiredHandler: memberExpiredHandler,
		coordinator:          protoGroup.Coordinator,
		epoch:                protoGroup.Epoch,
		recovered:            recovered,
		logger:               logger,
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	for _, member := range protoGroup.Members {
		group.addMember(member.Id, member.Streams)
	}
	group.debugLogAssignments()
	return group
}

// String returns a human-readable representation of the consumer group.
func (c *consumerGroup) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return fmt.Sprintf(
		"[id=%s, coordinator=%s, epoch=%d, members=%d]",
		c.id, c.coordinator, c.epoch, len(c.members))
}

// GetID returns the group's ID.
func (c *consumerGroup) GetID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.id
}

// SetCoordinator sets the coordinator for the group.
func (c *consumerGroup) SetCoordinator(coordinator string, epoch uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch < c.epoch {
		return fmt.Errorf("proposed group epoch %d is less than current epoch %d",
			epoch, c.epoch)
	}
	previousCoordinator := c.coordinator
	c.coordinator = coordinator
	c.epoch = epoch

	// If this server has become the coordinator, start liveness timers for all
	// members. If this server was previously the coordinator, cancel timers.
	if previousCoordinator == c.serverID {
		for _, member := range c.members {
			if member.timer != nil {
				member.timer.Stop()
				member.timer = nil
			}
		}
	}
	if coordinator == c.serverID {
		c.startMemberTimers()
	}

	return nil
}

// GetCoordinator returns the coordinator ID and epoch for the consumer group.
func (c *consumerGroup) GetCoordinator() (string, uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.coordinator, c.epoch
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
	if c.coordinator == c.serverID {
		c.startMemberTimers()
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
func (c *consumerGroup) AddMember(consumerID string, streams []string, epoch uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if epoch < c.epoch {
		return fmt.Errorf("proposed group epoch %d is less than current epoch %d",
			epoch, c.epoch)
	}

	c.addMember(consumerID, streams)

	c.epoch = epoch
	c.debugLogAssignments()
	return nil
}

func (c *consumerGroup) addMember(consumerID string, streams []string) {
	var timer *time.Timer

	// If this group is not in recovery mode and this server is the
	// coordinator, start a liveness timer for the consumer.
	if !c.recovered && c.coordinator == c.serverID {
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
	// Balance assignments.
	c.addConsumer(cons)
}

// RemoveMember removes the given consumer from the group. If this server is
// the group coordinator, this will stop the liveness timer for the consumer.
// Returns a bool indicating if this was the last member of the group.
func (c *consumerGroup) RemoveMember(consumerID string, epoch uint64) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch < c.epoch {
		return false, fmt.Errorf("proposed group epoch %d is less than current epoch %d",
			epoch, c.epoch)
	}

	consumer, ok := c.members[consumerID]
	if !ok {
		return false, ErrConsumerNotMember
	}
	if consumer.timer != nil {
		consumer.timer.Stop()
	}
	// Balance assignments.
	c.removeConsumer(consumer)
	delete(c.members, consumerID)
	c.epoch = epoch
	c.debugLogAssignments()
	return len(c.members) == 0, nil
}

// StreamDeleted is called whenever a stream is deleted.
func (c *consumerGroup) StreamDeleted(stream string, epoch uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch < c.epoch {
		return fmt.Errorf("proposed group epoch %d is less than current epoch %d",
			epoch, c.epoch)
	}

	subscribers, ok := c.subscribers[stream]
	if !ok {
		return nil
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
	// subscribed to. Range over the streams in order for deterministic
	// processing across servers.
	rangeStreamsOrdered(rebalance, func(stream string) {
		// Enforce heap invariants.
		subscribers, ok := c.subscribers[stream]
		if ok {
			heap.Init(subscribers)
		}
		c.balanceAssignmentsForStream(stream)
	})
	c.epoch = epoch
	c.debugLogAssignments()
	return nil
}

// GetAssignments returns the partition assignments for the given consumer
// along with the group epoch. It returns an error if the consumer is not
// a member of the group, if this server is not the group coordinator, or the
// provided epoch differs from the current known epoch.
func (c *consumerGroup) GetAssignments(consumerID string, epoch uint64) (
	partitionAssignments, uint64, error) {

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.coordinator != c.serverID {
		return nil, 0, ErrBrokerNotCoordinator
	}

	if epoch != c.epoch {
		return nil, 0, ErrGroupEpoch
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
	member.timer.Reset(c.consumerTimeout)

	assignments := make(partitionAssignments, len(member.assignments))
	for stream, partitions := range member.assignments {
		partitionAssignments := make([]int32, len(partitions))
		copy(partitionAssignments, partitions)
		assignments[stream] = partitionAssignments
	}

	return assignments, c.epoch, nil
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

// startMemberTimers starts liveness timers for all group members. This must be
// called within the group mutex.
func (c *consumerGroup) startMemberTimers() {
	for memberID, member := range c.members {
		// If for some reason the member already has a timer, cancel it.
		if member.timer != nil {
			member.timer.Stop()
		}
		member.timer = c.startMemberTimer(memberID)
	}
}

// startMemberTimer starts a liveness timer for the given member.
func (c *consumerGroup) startMemberTimer(consumerID string) *time.Timer {
	return time.AfterFunc(c.consumerTimeout, c.consumerExpired(consumerID))
}

// consumerExpired returns a callback function which is invoked when a consumer
// times out. This will attempt to remove the expired consumer from the group.
func (c *consumerGroup) consumerExpired(consumerID string) func() {
	return func() {
		c.logger.Errorf("Consumer %s timed out for consumer group %s, removing from group",
			consumerID, c.id)
		if err := c.memberExpiredHandler(c.id, consumerID); err != nil {
			c.logger.Errorf("Failed to remove consumer %s from consumer group %s: %v",
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

// addConsumer adds the given consumer to the group's subscriber heaps for
// consumer's streams. This will result in balancing the partition assignments
// for these streams. This must be called within the group mutex.
func (c *consumerGroup) addConsumer(cons *consumer) {
	// range over streams in order for deterministic processing across servers.
	rangeStreamsOrdered(cons.streams, func(stream string) {
		subscribers, ok := c.subscribers[stream]
		if !ok {
			subscribers = &consumerHeap{}
			c.subscribers[stream] = subscribers
		}
		heap.Push(subscribers, cons)
		c.balanceAssignmentsForStream(stream)
	})
}

// removeConsumer removes the given consumer from the group's subscriber heaps
// for consumer's streams. This will result in balancing the partition
// assignments for any streams this consumer had assignments for. This must be
// called within the group mutex.
func (c *consumerGroup) removeConsumer(cons *consumer) {
	// range over streams in order for deterministic processing across servers.
	rangeStreamsOrdered(cons.streams, func(stream string) {
		subscribers, ok := c.subscribers[stream]
		if !ok {
			return
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
	})
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

	c.logger.Debugf("Balancing consumer group %s assignments for stream %s (%s)",
		c.id, streamName, english.Plural(len(*subscribers), "subscriber", ""))

	// Reset assignments for stream.
	// TODO: This rebalancing could probably be implemented in a more optimized
	// way, e.g. avoiding unnecessary reassignments of partitions. For
	// instance, if there is excess capacity (more consumers than partitions),
	// there is no reason to reassign partitions.
	for _, subscriber := range *subscribers {
		subscriber.removeStreamAssignments(streamName)
	}
	heap.Init(subscribers)

	// Assign each partition to the consumer with the least amount of
	// assignments.
	for partition := int32(0); partition < c.getStreamPartitions(streamName); partition++ {
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

func (c *consumerGroup) debugLogAssignments() {
	if len(c.members) == 0 {
		return
	}
	msg := fmt.Sprintf("Partition assignments for consumer group %s (epoch: %d):\n", c.id, c.epoch)
	for id, consumer := range c.members {
		msg += fmt.Sprintf("  consumer %s\n", id)
		for stream, partitions := range consumer.assignments {
			partitionStrs := make([]string, len(partitions))
			for i, partition := range partitions {
				partitionStrs[i] = strconv.Itoa(int(partition))
			}
			msg += fmt.Sprintf("    %s: [%s]\n", stream, strings.Join(partitionStrs, ", "))
		}
	}
	c.logger.Debugf(msg)
}

func rangeStreamsOrdered(streams map[string]struct{}, f func(stream string)) {
	// First sort map keys.
	var (
		i             = 0
		sortedStreams = make([]string, len(streams))
	)
	for stream := range streams {
		sortedStreams[i] = stream
		i++
	}
	sort.Strings(sortedStreams)

	// Range over sorted keys.
	for _, stream := range sortedStreams {
		f(stream)
	}
}
