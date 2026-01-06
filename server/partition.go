package server

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	client "github.com/liftbridge-io/liftbridge-api/v2/go"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liftbridge-io/liftbridge/server/commitlog"
	encryption "github.com/liftbridge-io/liftbridge/server/encryption"
	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

// recvChannelSize specifies the size of the channel that feeds the leader
// message processing loop.
const recvChannelSize = 64 * 1024

// timestamp returns the current time in Unix nanoseconds. This function exists
// for mocking purposes.
var timestamp = func() int64 { return time.Now().UnixNano() }

// subscription tracks state for a partition subscription.
type subscription struct {
	mu     sync.Mutex
	closed chan struct{}
	msgs   chan *client.Message
	errors chan *status.Status
}

func (s *subscription) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.closed:
		return
	default:
	}
	close(s.closed)
}

func (s *subscription) Messages() <-chan *client.Message {
	return s.msgs
}

func (s *subscription) Errors() <-chan *status.Status {
	return s.errors
}

func (s *subscription) Closed() <-chan struct{} {
	return s.closed
}

// replica tracks the latest log offset for a particular partition replica.
type replica struct {
	mu     sync.RWMutex
	offset int64
}

// updateLatestOffset sets the replica's latest log offset if the given offset
// is greater than the current offset. It returns a bool indicating if the
// offset was updated or not.
func (r *replica) updateLatestOffset(offset int64) (updated bool) {
	r.mu.Lock()
	if offset > r.offset {
		r.offset = offset
		updated = true
	}
	r.mu.Unlock()
	return
}

// getLatestOffset returns the replica's latest log offset.
func (r *replica) getLatestOffset() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.offset
}

// EventTimestamps contains the first and latest times when an event has
// occurred.
type EventTimestamps struct {
	firstTime  time.Time // Time when the first event occurred.
	latestTime time.Time // Time when the latest event occurred.
}

// update should be called when an event has occurred. It updates the first and
// latest timestamps.
func (e *EventTimestamps) update() {
	timestamp := time.Now()
	if e.firstTime.IsZero() {
		e.firstTime = timestamp
	}
	e.latestTime = timestamp
}

// groupMember tracks state for a consumer group member.
type groupMember struct {
	consumerID string
	groupEpoch uint64
	sub        *subscription
}

// partition represents a replicated message stream partition backed by a
// durable commit log. A partition is attached to a NATS subject and stores
// messages on that subject in a file-backed log. A partition has a set of
// replicas assigned to it, which are the brokers responsible for replicating
// the partition. The ISR, or in-sync replicas set, is the set of replicas
// which are currently caught up with the partition leader's log. If a replica
// falls behind, it will be removed from the ISR. Followers replicate the
// leader's log by fetching messages from it. All partition access should go
// through exported methods.
type partition struct {
	mu                            sync.RWMutex
	closeMu                       sync.Mutex
	sub                           *nats.Subscription // Subscription to partition NATS subject
	leaderReplSub                 *nats.Subscription // Subscription for replication requests from followers
	leaderOffsetSub               *nats.Subscription // Subscription for leader epoch offset requests from followers
	log                           commitlog.CommitLog
	srv                           *Server
	isLeading                     bool
	isFollowing                   bool
	isClosed                      bool
	replicas                      map[string]struct{}
	isr                           map[string]*replica
	minISR                        int
	replicators                   map[string]*replicator
	commitQueue                   *queue.Queue
	commitCheck                   chan struct{}
	recovered                     bool
	stopFollower                  chan struct{}
	stopLeader                    chan struct{}
	notify                        chan struct{}
	belowMinISR                   bool
	pause                         bool // Pause replication on the leader (for unit testing)
	shutdown                      sync.WaitGroup
	paused                        bool
	autoPauseTime                 time.Duration
	autoPauseDisableIfSubscribers bool
	subscriberCount               int64
	messagesReceivedTimestamps    EventTimestamps // First and latest time a message was received on this partition
	pauseTimestamps               EventTimestamps // First and latest time this partition was paused or resumed
	readonlyTimestamps            EventTimestamps // First and latest time this partition had its read-only status changed
	encryptionHandler             encryption.Codec
	consumersMu                   sync.Mutex
	consumers                     map[string]*groupMember // Maps consumer groups to consumers
	*proto.Partition
}

// newPartition creates a new stream partition. If the partition is recovered,
// it should not be started until the recovery process has completed to avoid
// starting it in an intermediate state. This call will initialize or recover
// the partition's backing commit log or return an error if it fails to do so.
//
// A partitioned stream maps to separate NATS subjects: subject, subject.1,
// subject.2, etc.
func (s *Server) newPartition(protoPartition *proto.Partition, recovered bool, config *proto.StreamConfig) (*partition, error) {
	streamsConfig := &StreamsConfig{
		SegmentMaxBytes:               s.config.Streams.SegmentMaxBytes,
		SegmentMaxAge:                 s.config.Streams.SegmentMaxAge,
		RetentionMaxBytes:             s.config.Streams.RetentionMaxBytes,
		RetentionMaxMessages:          s.config.Streams.RetentionMaxMessages,
		RetentionMaxAge:               s.config.Streams.RetentionMaxAge,
		CleanerInterval:               s.config.Streams.CleanerInterval,
		Compact:                       s.config.Streams.Compact,
		CompactMaxGoroutines:          s.config.Streams.CompactMaxGoroutines,
		AutoPauseTime:                 s.config.Streams.AutoPauseTime,
		AutoPauseDisableIfSubscribers: s.config.Streams.AutoPauseDisableIfSubscribers,
		MinISR:                        s.config.Clustering.MinISR,
		Encryption:                    s.config.Streams.Encryption,
	}
	streamsConfig.ApplyOverrides(config)
	var (
		file = filepath.Join(s.config.DataDir, "streams", protoPartition.Stream,
			strconv.FormatInt(int64(protoPartition.Id), 10))
		name = fmt.Sprintf("[subject=%s, stream=%s, partition=%d]",
			protoPartition.Subject, protoPartition.Stream, protoPartition.Id)

		log, err = commitlog.New(commitlog.Options{
			Name:                 name,
			Path:                 file,
			MaxSegmentBytes:      streamsConfig.SegmentMaxBytes,
			MaxSegmentAge:        streamsConfig.SegmentMaxAge,
			MaxLogBytes:          streamsConfig.RetentionMaxBytes,
			MaxLogMessages:       streamsConfig.RetentionMaxMessages,
			MaxLogAge:            streamsConfig.RetentionMaxAge,
			CleanerInterval:      streamsConfig.CleanerInterval,
			Compact:              streamsConfig.Compact,
			CompactMaxGoroutines: streamsConfig.CompactMaxGoroutines,
			Logger:               s.logger,
			ConcurrencyControl:   streamsConfig.ConcurrencyControl,
		})
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create commit log")
	}

	replicas := make(map[string]struct{}, len(protoPartition.Replicas))
	for _, replica := range protoPartition.Replicas {
		replicas[replica] = struct{}{}
	}

	isr := make(map[string]*replica, len(protoPartition.Isr))
	for _, rep := range protoPartition.Isr {
		offset := int64(-1)
		// For this server, initialize the replica offset to the newest offset.
		if rep == s.config.Clustering.ServerID {
			offset = log.NewestOffset()
		}
		isr[rep] = &replica{offset: offset}
	}

	st := &partition{
		Partition:                     protoPartition,
		log:                           log,
		srv:                           s,
		replicas:                      replicas,
		isr:                           isr,
		minISR:                        streamsConfig.MinISR,
		commitCheck:                   make(chan struct{}, len(protoPartition.Replicas)),
		notify:                        make(chan struct{}, 1),
		recovered:                     recovered,
		autoPauseTime:                 streamsConfig.AutoPauseTime,
		autoPauseDisableIfSubscribers: streamsConfig.AutoPauseDisableIfSubscribers,
		consumers:                     make(map[string]*groupMember),
	}

	if streamsConfig.Encryption {
		// Init handler for Encryption-at-Rest

		encryptionHandler, err := encryption.NewLocalEncryptionHandler()

		if err != nil {
			return nil, errors.Wrap(err, "Failed to initialize encryption handler on partition")
		}

		st.encryptionHandler = encryptionHandler
	}

	return st, nil
}

// replacePartition creates a new stream partition to replace another one. The
// old partition's events timestamps are kept.
func (s *Server) replacePartition(oldPartition *partition, recovered bool, config *proto.StreamConfig) (*partition, error) {
	st, err := s.newPartition(oldPartition.Partition, recovered, config)

	if err == nil {
		st.messagesReceivedTimestamps = oldPartition.MessagesReceivedTimestamps()
		st.pauseTimestamps = oldPartition.PauseTimestamps()
		st.readonlyTimestamps = oldPartition.ReadonlyTimestamps()
	}

	return st, err
}

// String returns a human-readable string representation of the partition.
func (p *partition) String() string {
	return fmt.Sprintf("[subject=%s, stream=%s, partition=%d]", p.Subject, p.Stream, p.Id)
}

// close stops the partition if it is running and closes the commit log. Must
// be called within the scope of the partition mutex.
func (p *partition) close() error {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()

	if p.isClosed {
		return nil
	}

	if err := p.log.Close(); err != nil {
		return err
	}

	if err := p.stopLeadingOrFollowing(); err != nil {
		return err
	}

	p.isClosed = true
	return nil
}

// Close stops the partition if it is running and closes the commit log.
func (p *partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.close()
}

// Pause stops the partition if it is running, closes the commit log and sets
// the paused flag.
func (p *partition) Pause() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.paused = true
	p.Paused = true // Also set the protobuf value (used for snapshotting)
	p.pauseTimestamps.update()

	return p.close()
}

// IsPaused indicates if the partition is currently paused.
func (p *partition) IsPaused() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.paused
}

// SetReadonly enables or disables readonly for the partition. When enabled,
// new messages cannot be written to the log and consumers will not block once
// they reach the end of the log. This does not affect replication.
func (p *partition) SetReadonly(readonly bool) {
	p.log.SetReadonly(readonly)

	p.mu.Lock()
	defer p.mu.Unlock()

	p.Readonly = readonly // Also set the protobuf value (used for snapshotting)
	p.readonlyTimestamps.update()
}

// IsReadonly indicates if the partition is currently readonly.
func (p *partition) IsReadonly() bool {
	return p.log.IsReadonly()
}

// GetGroupConsumer returns the consumer for the given group or nil if no
// consumer is subscribed.
func (p *partition) GetGroupConsumer(groupID string) *groupMember {
	p.consumersMu.Lock()
	defer p.consumersMu.Unlock()

	return p.consumers[groupID]
}

// Delete stops the partition if it is running, closes, and deletes the commit
// log.
func (p *partition) Delete() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.log.Delete(); err != nil {
		return err
	}

	return p.stopLeadingOrFollowing()
}

// Subscribe sets up a subscription on the partition and begins sending
// messages on the returned channel. The subscription will run until the cancel
// channel is closed, the context is canceled, or an error is returned
// asynchronously on the status channel. If the subscriber is part of a
// consumer group, this will ensure only one member of the group is subscribed
// to the partition at a time.
func (p *partition) Subscribe(ctx context.Context, req *client.SubscribeRequest) (
	*subscription, *status.Status) {

	var (
		previousSubscriber *groupMember
		groupID            string
		consumerID         string
		groupEpoch         uint64
	)

	if req.Consumer != nil {
		groupID = req.Consumer.GroupId
		consumerID = req.Consumer.ConsumerId
		groupEpoch = req.Consumer.GroupEpoch
	}

	if groupID != "" {
		// Grab the consumers mutex if this subscriber is part of a consumer
		// group.
		p.consumersMu.Lock()
		defer p.consumersMu.Unlock()

		// If there is an existing member of the group subscribed to the
		// partition, check if the new subscriber has a more recent group
		// epoch. If it does, it will replace the existing consumer.
		existing, ok := p.consumers[groupID]
		if ok {
			if existing.groupEpoch > groupEpoch {
				return nil, status.New(codes.FailedPrecondition,
					"Consumer is not currently assigned this partition")
			}
			previousSubscriber = existing
		}
	}

	startOffset, st := p.getStartOffset(req)
	if st != nil {
		return nil, st
	}

	stopOffset, st := p.getStopOffset(req)
	if st != nil {
		return nil, st
	}

	if stopOffset != waitForNewMessages && stopOffset < startOffset {
		return nil, status.New(
			codes.InvalidArgument, fmt.Sprintf("Stop offset is before start offset: %d < %d",
				stopOffset, startOffset))
	}

	// Cancel previous group subscriber if there was one.
	if previousSubscriber != nil {
		p.srv.logger.Debugf("Replacing group %s consumer %s with consumer %s for partition %s",
			groupID, previousSubscriber.consumerID, consumerID, p)
		previousSubscriber.sub.Close()
	}

	var (
		ch     = make(chan *client.Message)
		errCh  = make(chan *status.Status)
		reader commitlog.MessageReader
		err    error
	)

	if req.Reverse {
		reader, err = p.log.NewReverseReader(startOffset, false)
	} else {
		reader, err = p.log.NewReader(startOffset, false)
	}
	if err != nil {
		return nil, status.New(
			codes.Internal, fmt.Sprintf("Failed to create stream reader: %v", err))
	}

	cancel := make(chan struct{})
	p.srv.startGoroutine(p.newSubscribeLoop(ctx, groupID, consumerID, reader,
		stopOffset, ch, errCh, cancel, req.Reverse))

	sub := &subscription{
		closed: cancel,
		msgs:   ch,
		errors: errCh,
	}

	if groupID != "" {
		p.consumers[groupID] = &groupMember{
			consumerID: consumerID,
			groupEpoch: groupEpoch,
			sub:        sub,
		}
	}

	return sub, nil
}

// newSubscribeLoop returns a function to be called in a goroutine which starts
// the subscription loop.
func (p *partition) newSubscribeLoop(ctx context.Context, groupID, consumerID string,
	reader commitlog.MessageReader, stopOffset int64, ch chan<- *client.Message, errCh chan<- *status.Status,
	cancel <-chan struct{}, reverse bool) func() {

	return func() {
		// Update the active subscriber count.
		p.increaseSubscriberCount()
		defer p.decreaseSubscriberCount()
		if groupID != "" {
			defer p.removeGroupSubscriber(groupID, consumerID)
		}

		headersBuf := make([]byte, 28)
		for {
			// TODO: this could be more efficient.
			m, offset, timestamp, _, err := reader.ReadMessage(ctx, headersBuf)

			if err != nil {
				var s *status.Status
				if err == commitlog.ErrCommitLogDeleted {
					// Partition was deleted while subscribed.
					s = status.New(codes.NotFound, err.Error())
				} else if err == commitlog.ErrCommitLogClosed {
					// Partition was closed while subscribed (likely paused).
					code := codes.Internal
					if p.IsPaused() {
						code = codes.FailedPrecondition
					}
					s = status.New(code, err.Error())
				} else if err == commitlog.ErrCommitLogReadonly {
					// Partition was set to readonly while subscribed.
					s = status.New(codes.ResourceExhausted, "End of readonly partition")
				} else {
					s = status.Convert(err)
				}

				select {
				case errCh <- s:
				case <-cancel:
				}
				return
			}
			msgValue := m.Value()

			headers := m.Headers()

			// Data decryption
			if p.encryptionHandler != nil {
				// Decryption of data on server side
				decryptedMsg, err := p.encryptionHandler.Read(msgValue)

				if err != nil {
					s := status.Convert(err)
					select {
					case errCh <- s:
					case <-cancel:
					}
					return
				}

				msgValue = decryptedMsg
			}

			var (
				msg = &client.Message{
					Stream:       p.Stream,
					Partition:    p.Id,
					Offset:       offset,
					Key:          m.Key(),
					Value:        msgValue,
					Timestamp:    timestamp,
					Headers:      headers,
					Subject:      string(headers["subject"]),
					ReplySubject: string(headers["reply"]),
				}
			)
			select {
			case ch <- msg:
			case <-cancel:
				return
			}
			if offset == stopOffset {
				s := status.New(codes.ResourceExhausted, "Stop offset reached")

				select {
				case errCh <- s:
				case <-cancel:
				}
				return
			}
		}
	}
}

func (p *partition) removeGroupSubscriber(groupID, consumerID string) {
	p.consumersMu.Lock()
	defer p.consumersMu.Unlock()
	sub, ok := p.consumers[groupID]
	if !ok {
		return
	}
	if sub.consumerID == consumerID {
		delete(p.consumers, groupID)
	}
}

func (p *partition) getStartOffset(req *client.SubscribeRequest) (int64, *status.Status) {
	var startOffset int64
	switch req.StartPosition {
	case client.StartPosition_OFFSET:
		startOffset = req.StartOffset
	case client.StartPosition_TIMESTAMP:
		offset, err := p.log.EarliestOffsetAfterTimestamp(req.StartTimestamp)
		if err != nil {
			return startOffset, status.New(
				codes.Internal, fmt.Sprintf("Failed to lookup offset for timestamp: %v", err))
		}
		startOffset = offset
	case client.StartPosition_EARLIEST:
		startOffset = p.log.OldestOffset()
	case client.StartPosition_LATEST:
		startOffset = p.log.NewestOffset()
	case client.StartPosition_NEW_ONLY:
		startOffset = p.log.NewestOffset() + 1
	default:
		return startOffset, status.New(
			codes.InvalidArgument,
			fmt.Sprintf("Unknown StartPosition %s", req.StartPosition))
	}

	// If log is empty, next offset will be 0.
	if startOffset < 0 {
		startOffset = 0
	}

	return startOffset, nil
}

func (p *partition) getStopOffset(req *client.SubscribeRequest) (int64, *status.Status) {
	var stopOffset int64
	switch req.StopPosition {
	case client.StopPosition_STOP_ON_CANCEL:
		stopOffset = waitForNewMessages
		if p.log.IsReadonly() {
			stopOffset = p.log.NewestOffset()
		}
	case client.StopPosition_STOP_OFFSET:
		stopOffset = req.StopOffset
	case client.StopPosition_STOP_TIMESTAMP:
		var err error
		stopOffset, err = p.log.LatestOffsetBeforeTimestamp(req.StopTimestamp)
		if err != nil {
			return stopOffset, status.New(
				codes.Internal, fmt.Sprintf("Failed to lookup offset for timestamp: %v", err))
		}
	case client.StopPosition_STOP_LATEST:
		stopOffset = p.log.NewestOffset()
		if stopOffset == -1 {
			return stopOffset, status.New(codes.ResourceExhausted, "Stream is empty")
		}
	default:
		return stopOffset, status.New(
			codes.InvalidArgument,
			fmt.Sprintf("Unknown StopPosition %s", req.StopPosition))
	}

	return stopOffset, nil
}

// increaseSubscriberCount increases the number of subscribers. Partitions with
// a subscriber count greater than zero will not be auto-paused if the
// partition is idle, and the corresponding configuration option is set.
func (p *partition) increaseSubscriberCount() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.subscriberCount++
}

// decreaseSubscriberCount decreases the number of subscribers. Partitions with
// a subscriber count greater than zero will not be auto-paused if the
// partition is idle, and the corresponding configuration option is set.
func (p *partition) decreaseSubscriberCount() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.subscriberCount--
	if p.subscriberCount < 0 {
		p.subscriberCount = 0
		p.srv.logger.Errorf("Negative partition subscriber count for partition %s: %d",
			p, p.subscriberCount)
	}
}

// MessagesReceivedTimestamps returns the first and latest times a message was
// received on this partition.
func (p *partition) MessagesReceivedTimestamps() EventTimestamps {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.messagesReceivedTimestamps
}

// PauseTimestamps returns the first and latest time this partition was paused
// or resumed.
func (p *partition) PauseTimestamps() EventTimestamps {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.pauseTimestamps
}

// ReadonlyTimestamps returns the first and latest time this partition had its
// read-only status changed.
func (p *partition) ReadonlyTimestamps() EventTimestamps {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.readonlyTimestamps
}

// Notify is used to short circuit the sleep backoff a partition uses when it
// has replicated to the end of the leader's log (i.e. the log end offset).
// When a follower reaches the end of the log, it starts to sleep in between
// replication requests to avoid overloading the leader. However, this causes
// added commit latency when new messages are published to the log since the
// follower is idle. As a result, the leader will note when a follower is
// caught up and send a notification in order to wake an idle follower back up
// when new data is written to the log.
func (p *partition) Notify() {
	if p.IsLeader() {
		// If we are now the leader, do nothing.
		return
	}
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// SetLeader sets the leader for the partition to the given replica and leader
// epoch. If the partition's current leader epoch is greater than the given
// epoch, this returns an error. This will also start the partition as a leader
// or follower, if applicable, unless the partition is in recovery mode or
// paused.
func (p *partition) SetLeader(leader string, epoch uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if epoch < p.LeaderEpoch {
		return fmt.Errorf("proposed leader epoch %d is less than current epoch %d",
			epoch, p.LeaderEpoch)
	}
	p.Leader = leader
	p.LeaderEpoch = epoch

	if p.recovered || p.paused {
		// If this partition is being recovered, we will start the
		// leader/follower loop later. If it's paused, we won't start it til
		// it's resumed.
		return nil
	}

	return p.startLeadingOrFollowing()
}

// StartRecovered starts the partition as a leader or follower, if applicable,
// if it's in recovery mode. This should be called for each partition after the
// recovery process completes. If the partition is paused, this will be a
// no-op.
func (p *partition) StartRecovered() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.recovered {
		return false, nil
	}
	if p.paused {
		return true, nil
	}
	if err := p.startLeadingOrFollowing(); err != nil {
		return false, err
	}
	p.recovered = false
	return true, nil
}

// startLeadingOrFollowing starts the partition as a leader or follower, if
// applicable.
func (p *partition) startLeadingOrFollowing() error {
	if p.Leader == p.srv.config.Clustering.ServerID {
		p.srv.logger.Debugf("Server becoming leader for partition %s, epoch: %d", p, p.LeaderEpoch)
		if err := p.becomeLeader(p.LeaderEpoch); err != nil {
			p.srv.logger.Errorf("Server failed becoming leader for partition %s: %v", p, err)
			return err
		}
	} else if p.inReplicas(p.srv.config.Clustering.ServerID) {
		p.srv.logger.Debugf("Server becoming follower for partition %s, epoch: %d", p, p.LeaderEpoch)
		if err := p.becomeFollower(); err != nil {
			p.srv.logger.Errorf("Server failed becoming follower for partition %s: %v", p, err)
			return err
		}
	}
	return nil
}

// stopLeadingOrFollowing stops the partition as a leader or follower, if
// applicable. Must be called within the scope of the partition mutex.
func (p *partition) stopLeadingOrFollowing() error {
	if p.isFollowing {
		// Stop following if previously a follower.
		if err := p.stopFollowing(); err != nil {
			return err
		}
	} else if p.isLeading {
		// If previously a leader, we need to reset.
		if err := p.stopLeading(); err != nil {
			return err
		}
	}
	return nil
}

// GetLeader returns the replica that is the partition leader and the leader
// epoch.
func (p *partition) GetLeader() (string, uint64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Leader, p.LeaderEpoch
}

// IsLeader indicates if this server is the partition leader.
func (p *partition) IsLeader() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isLeading
}

// becomeLeader is called when the server has become the leader for this
// partition.
func (p *partition) becomeLeader(epoch uint64) error {
	if err := p.stopLeadingOrFollowing(); err != nil {
		return err
	}

	if !p.recovered {
		// Update leader epoch on log if this isn't a recovered partition. A
		// recovered partition indicates we were the previous leader and are
		// continuing a leader epoch.
		if err := p.log.NewLeaderEpoch(epoch); err != nil {
			return errors.Wrap(err, "failed to update leader epoch on log")
		}
	}

	// Update this replica's latest offset to ensure it's up to date.
	rep, ok := p.isr[p.srv.config.Clustering.ServerID]
	if !ok {
		// This shouldn't happen - a leader should always be in the ISR.
		// Handle gracefully for corrupt snapshots (see #354).
		p.srv.logger.Warnf("Leader %s not found in ISR for partition %s, adding self to ISR",
			p.srv.config.Clustering.ServerID, p)
		rep = &replica{offset: -1}
		p.isr[p.srv.config.Clustering.ServerID] = rep
		// Also update the protobuf ISR list for persistence.
		p.Isr = append(p.Isr, p.srv.config.Clustering.ServerID)
	}
	rep.updateLatestOffset(p.log.NewestOffset())

	// Start message processing loop.
	recvChan := make(chan *nats.Msg, recvChannelSize)
	p.stopLeader = make(chan struct{})
	p.srv.startGoroutineWithArgsWG(func(args ...interface{}) {
		stop := args[0].(chan struct{})
		p.messageProcessingLoop(recvChan, stop, epoch)
	}, p.shutdown, p.stopLeader)

	// Start replicating to followers.
	p.startReplicating(epoch, p.stopLeader)

	// Subscribe to the NATS subject and begin sequencing messages.
	// TODO: This should be drained on shutdown.
	sub, err := p.srv.nc.QueueSubscribe(p.getSubject(), p.Group, func(m *nats.Msg) {
		recvChan <- m
	})
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to NATS")
	}
	sub.SetPendingLimits(-1, -1)
	p.sub = sub
	p.srv.nc.Flush()

	// Subscribe to the partition replication subject.
	sub, err = p.srv.ncRepl.Subscribe(p.getReplicationRequestInbox(), p.handleReplicationRequest)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to replication inbox")
	}
	sub.SetPendingLimits(-1, -1)
	p.leaderReplSub = sub

	// Also subscribe to leader epoch offset requests subject.
	sub, err = p.srv.ncRepl.Subscribe(p.getLeaderOffsetRequestInbox(), p.handleLeaderOffsetRequest)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to replication inbox")
	}
	sub.SetPendingLimits(-1, -1)
	p.leaderOffsetSub = sub
	p.srv.ncRepl.Flush()

	// Start auto-pause timer if enabled.
	if p.autoPauseTime > 0 {
		p.srv.startGoroutine(func() {
			p.autoPauseLoop(p.stopLeader)
		})
	}

	p.isLeading = true
	p.isFollowing = false

	// Notify the cursor manager if we've become leader for a cursor partition.
	if p.Stream == cursorsStream {
		p.srv.cursors.BecomePartitionLeader()
	}

	return nil
}

// stopLeading causes the partition to step down as leader by unsubscribing
// from the NATS subject and replication subject, stopping message processing
// and replication, and disposing the commit queue. Must be called within the
// scope of the partition mutex.
func (p *partition) stopLeading() error {
	// Unsubscribe from NATS subject.
	if err := p.sub.Unsubscribe(); err != nil {
		return err
	}

	// Unsubscribe from replication subject.
	if err := p.leaderReplSub.Unsubscribe(); err != nil {
		return err
	}

	// Unsubscribe from leader epoch offset subject.
	if err := p.leaderOffsetSub.Unsubscribe(); err != nil {
		return err
	}

	// Stop processing messages and replicating.
	close(p.stopLeader)

	// Wait for loops to shutdown. Release mutex while we wait to avoid
	// deadlocks.
	p.mu.Unlock()
	p.shutdown.Wait()
	p.mu.Lock()

	p.commitQueue.Dispose()
	p.isLeading = false

	return nil
}

// becomeFollower is called when the server has become a follower for this
// partition.
func (p *partition) becomeFollower() error {
	if err := p.stopLeadingOrFollowing(); err != nil {
		return err
	}

	// Truncate potentially uncommitted messages from the log.
	if err := p.truncateUncommitted(); err != nil {
		return errors.Wrap(err, "failed to truncate log")
	}

	// Start fetching messages from the leader's log starting at the HW.
	p.stopFollower = make(chan struct{})
	p.srv.logger.Debugf("Replicating partition %s from leader %s", p, p.Leader)
	p.srv.startGoroutine(func() {
		p.replicationRequestLoop(p.Leader, p.LeaderEpoch, p.stopFollower)
	})

	p.isFollowing = true
	p.isLeading = false

	return nil
}

// stopFollowing causes the partition to step down as a follower by stopping
// replication requests and the leader failure detector.
func (p *partition) stopFollowing() error {
	// Stop replication request and leader failure detector loop.
	// TODO: Do graceful shutdown similar to stopLeading().
	close(p.stopFollower)
	p.isFollowing = false
	return nil
}

// handleLeaderOffsetRequest is a NATS handler that's invoked when the leader
// receives a leader epoch offset request from a follower. The request will
// contain the latest leader epoch in the follower's leader epoch sequence.
// This will send the last offset for the requested leader epoch, i.e. the
// start offset of the first leader epoch larger than the requested leader
// epoch or the log end offset if the leader's current epoch is equal to the
// one requested.
func (p *partition) handleLeaderOffsetRequest(msg *nats.Msg) {
	req, err := proto.UnmarshalLeaderEpochOffsetRequest(msg.Data)
	if err != nil {
		p.srv.logger.Errorf("Invalid leader epoch offset request for partition %s: %v", p, err)
		return
	}
	resp, err := proto.MarshalLeaderEpochOffsetResponse(&proto.LeaderEpochOffsetResponse{
		EndOffset: p.log.LastOffsetForLeaderEpoch(req.LeaderEpoch),
	})
	if err != nil {
		panic(err)
	}
	if err := msg.Respond(resp); err != nil {
		p.srv.logger.Errorf("Failed to respond to leader offset request: %v", err)
	}
}

// handleReplicationRequest is a NATS handler that's invoked when the leader
// receives a replication request from a follower. It will send messages to the
// NATS subject specified on the request.
func (p *partition) handleReplicationRequest(msg *nats.Msg) {
	received := time.Now()
	req, err := proto.UnmarshalReplicationRequest(msg.Data)
	if err != nil {
		p.srv.logger.Errorf("Invalid replication request for partition %s: %v", p, err)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pause {
		return
	}
	if req.LeaderEpoch != 0 && req.LeaderEpoch != p.LeaderEpoch {
		// This could indicate either another leader was elected (e.g. if this
		// node was somehow partitioned from the rest of the ISR) or the
		// follower is still trying to replicate from a previous leader. In
		// either case, drop the request.
		p.srv.logger.Warnf("Received replication request for partition %s from replica %s "+
			"in leader epoch %d, but current leader epoch is %d",
			p, req.ReplicaID, req.LeaderEpoch, p.LeaderEpoch)
		return
	}
	if _, ok := p.replicas[req.ReplicaID]; !ok {
		p.srv.logger.Warnf("Received replication request for partition %s from non-replica %s",
			p, req.ReplicaID)
		return
	}
	replicator, ok := p.replicators[req.ReplicaID]
	if !ok {
		panic(fmt.Sprintf("No replicator for partition %s and replica %s", p, req.ReplicaID))
	}
	replicator.request(replicationRequest{req, msg, received})
}

// handleReplicationResponse is a NATS handler that's invoked when a follower
// receives a replication response from the leader. This response will contain
// the leader epoch, leader HW, and (optionally) messages to replicate.
func (p *partition) handleReplicationResponse(msg *nats.Msg) int {
	leaderEpoch, hw, data, err := proto.UnmarshalReplicationResponse(msg.Data)
	if err != nil {
		p.srv.logger.Warnf("Invalid replication response for partition %s: %s", p, err)
		return 0
	}

	p.mu.RLock()
	if !p.isFollowing {
		p.mu.RUnlock()
		return 0
	}

	if p.LeaderEpoch != leaderEpoch {
		p.mu.RUnlock()
		return 0
	}
	p.mu.RUnlock()

	// Update HW from leader's HW.
	p.log.SetHighWatermark(hw)

	if len(data) == 0 {
		return 0
	}

	// We should have at least 28 bytes for headers.
	if len(data) <= 28 {
		p.srv.logger.Warnf("Invalid replication response for partition %s", p)
		return 0
	}
	offset := int64(proto.Encoding.Uint64(data[:8]))
	if offset < p.log.NewestOffset()+1 {
		return 0
	}
	offsets, err := p.log.AppendMessageSet(data)
	if err != nil {
		panic(fmt.Errorf("Failed to replicate data to log %s: %v", p, err))
	}
	return len(offsets)
}

// getReplicationRequestInbox returns the NATS subject to send replication
// requests to.
func (p *partition) getReplicationRequestInbox() string {
	return fmt.Sprintf("%s.%s.%d.replicate",
		p.srv.config.Clustering.Namespace, p.Stream, p.Id)
}

// getLeaderOffsetRequestInbox returns the NATS subject to send leader epoch
// offset requests to.
func (p *partition) getLeaderOffsetRequestInbox() string {
	return fmt.Sprintf("%s.%s.%d.offset",
		p.srv.config.Clustering.Namespace, p.Stream, p.Id)
}

// autoPauseLoop is a long-running loop the leader runs to check if the
// partition should be automatically paused due to inactivity.
func (p *partition) autoPauseLoop(stop <-chan struct{}) {
	timer := time.NewTimer(p.autoPauseTime)
	defer timer.Stop()
	for {
		select {
		case <-stop:
			return
		case <-timer.C:
		}

		p.mu.RLock()
		latestReceivedElapsed := time.Since(p.messagesReceivedTimestamps.latestTime)
		subsAllowPausing := !p.autoPauseDisableIfSubscribers || p.subscriberCount == 0
		p.mu.RUnlock()

		if latestReceivedElapsed > p.autoPauseTime && subsAllowPausing {
			p.srv.logger.Infof("Partition %s has not received a message in over %s, "+
				"auto pausing partition", p, p.autoPauseTime)
			if err := p.requestPause(); err != nil {
				p.srv.logger.Errorf("Failed to auto pause partition %s: %v", p, err)
			}
		}

		timer.Reset(computeTick(latestReceivedElapsed, p.autoPauseTime))
	}
}

// requestPause sends a request to pause the partition.
func (p *partition) requestPause() error {
	if e := p.srv.metadata.PauseStream(context.Background(), &proto.PauseStreamOp{
		Stream:     p.Stream,
		Partitions: []int32{p.Id},
		ResumeAll:  false,
	}); e != nil {
		return e.Err()
	}
	return nil
}

// messageProcessingLoop is a long-running loop that processes messages
// received on the given channel until the stop channel is closed. This will
// attempt to batch messages up before writing them to the commit log. Once
// written to the write-ahead log, a marker is written to the commit queue to
// indicate it's pending commit. Once the ISR has replicated the message, the
// leader commits it by removing it from the queue and sending an
// acknowledgement to the client.
func (p *partition) messageProcessingLoop(recvChan <-chan *nats.Msg, stop <-chan struct{},
	leaderEpoch uint64) {

	var (
		msg       *nats.Msg
		batchSize = p.srv.config.BatchMaxMessages
		batchWait = p.srv.config.BatchMaxTime
		msgBatch  = make([]*commitlog.Message, 0, batchSize)
	)
	// If Concurrency Control is enabled, then the message will be appended one by one.
	// This is to ensure no conflict between each message.
	if p.log.IsConcurrencyControlEnabled() {
		batchSize = 1
	}

	for {
		msgBatch = msgBatch[:0]
		select {
		case <-stop:
			return
		case msg = <-recvChan:
		}

		p.mu.Lock()
		p.messagesReceivedTimestamps.update()
		p.mu.Unlock()

		m := natsToProtoMessage(msg, leaderEpoch)

		if p.encryptionHandler != nil {
			// Encrypt value
			encryptedValue, err := p.encryptionHandler.Seal(m.Value)

			if err != nil {
				ack := &client.Ack{
					Stream:             p.Stream,
					PartitionSubject:   p.Subject,
					MsgSubject:         string(m.Headers["subject"]),
					AckInbox:           m.AckInbox,
					CorrelationId:      m.CorrelationID,
					AckPolicy:          m.AckPolicy,
					ReceptionTimestamp: m.Timestamp,
					AckError:           client.Ack_ENCRYPTION,
				}

				p.sendAck(ack)
				p.srv.logger.Errorf("Failed to encrypt message %s: %v", p, err)
				continue
			}
			// Set encrypted value
			m.Value = encryptedValue
		}

		// Reject messages that are larger than the max replication size.
		if int64(len(msg.Data)) > p.srv.config.Clustering.ReplicationMaxBytes {
			p.sendTooLargeNack(m)
			continue
		}
		msgBatch = append(msgBatch, m)
		remaining := batchSize - 1

		// Fill the batch up to the max batch size or until timeout.
		// Use a timer-based approach for efficient batching.
		var batchTimer *time.Timer
		var batchTimerC <-chan time.Time
		if batchWait > 0 {
			batchTimer = time.NewTimer(batchWait)
			batchTimerC = batchTimer.C
		}

	batchLoop:
		for remaining > 0 {
			// First, drain any immediately available messages without blocking
			select {
			case msg = <-recvChan:
				m := natsToProtoMessage(msg, leaderEpoch)

				if p.encryptionHandler != nil {
					encryptedValue, err := p.encryptionHandler.Seal(m.Value)
					if err != nil {
						ack := &client.Ack{
							Stream:             p.Stream,
							PartitionSubject:   p.Subject,
							MsgSubject:         string(m.Headers["subject"]),
							AckInbox:           m.AckInbox,
							CorrelationId:      m.CorrelationID,
							AckPolicy:          m.AckPolicy,
							ReceptionTimestamp: m.Timestamp,
							AckError:           client.Ack_ENCRYPTION,
						}
						p.sendAck(ack)
						p.srv.logger.Errorf("Failed to encrypt message %s: %v", p, err)
						continue batchLoop
					}
					m.Value = encryptedValue
				}
				if int64(len(msg.Data)) > p.srv.config.Clustering.ReplicationMaxBytes {
					p.sendTooLargeNack(m)
					continue batchLoop
				}
				msgBatch = append(msgBatch, m)
				remaining--
			default:
				// No more immediately available messages
				if batchWait == 0 {
					// No batch wait configured, dispatch now
					break batchLoop
				}
				// Wait for more messages or timeout
				select {
				case msg = <-recvChan:
					m := natsToProtoMessage(msg, leaderEpoch)

					if p.encryptionHandler != nil {
						encryptedValue, err := p.encryptionHandler.Seal(m.Value)
						if err != nil {
							ack := &client.Ack{
								Stream:             p.Stream,
								PartitionSubject:   p.Subject,
								MsgSubject:         string(m.Headers["subject"]),
								AckInbox:           m.AckInbox,
								CorrelationId:      m.CorrelationID,
								AckPolicy:          m.AckPolicy,
								ReceptionTimestamp: m.Timestamp,
								AckError:           client.Ack_ENCRYPTION,
							}
							p.sendAck(ack)
							p.srv.logger.Errorf("Failed to encrypt message %s: %v", p, err)
							continue batchLoop
						}
						m.Value = encryptedValue
					}
					if int64(len(msg.Data)) > p.srv.config.Clustering.ReplicationMaxBytes {
						p.sendTooLargeNack(m)
						continue batchLoop
					}
					msgBatch = append(msgBatch, m)
					remaining--
				case <-batchTimerC:
					// Batch timeout reached, dispatch what we have
					break batchLoop
				case <-stop:
					if batchTimer != nil {
						batchTimer.Stop()
					}
					return
				}
			}
		}

		// Stop the timer if it was created
		if batchTimer != nil {
			batchTimer.Stop()
		}

		// Write uncommitted messages to log.
		offsets, err := p.log.Append(msgBatch)
		if err != nil {

			// AckErr should be dispatched if ErrIncorrectOffset is raised.
			if errors.Is(err, commitlog.ErrIncorrectOffset) {
				msg := msgBatch[0]
				ack := &client.Ack{
					Stream:             p.Stream,
					PartitionSubject:   p.Subject,
					MsgSubject:         string(msg.Headers["subject"]),
					AckInbox:           msg.AckInbox,
					CorrelationId:      msg.CorrelationID,
					AckPolicy:          msg.AckPolicy,
					ReceptionTimestamp: msg.Timestamp,
					AckError:           client.Ack_INCORRECT_OFFSET,
				}

				p.sendAck(ack)
			}
			p.srv.logger.Errorf("Failed to append to log %s: %v", p, err)
			continue
		}

		// Track if we can use the fast path (RF=1 with no AckPolicy_ALL messages).
		useFastPath := p.ReplicationFactor == 1
		for i, msg := range msgBatch {
			if msg.AckPolicy == client.AckPolicy_ALL {
				useFastPath = false
			}
			p.processPendingMessage(offsets[i], msg)
		}

		// Fast path for RF=1: update high watermark once per batch instead of
		// going through the commit queue. This avoids queue overhead when there's
		// no replication to coordinate.
		if useFastPath {
			p.log.SetHighWatermark(offsets[len(offsets)-1])
		}

		// Update this replica's latest offset.
		p.updateISRLatestOffset(
			p.srv.config.Clustering.ServerID,
			offsets[len(offsets)-1],
		)
	}
}

// processPendingMessage sends an ack if the message's AckPolicy is LEADER and
// adds the pending message to the commit queue. Messages are removed from the
// queue and committed when the entire ISR has replicated them.
func (p *partition) processPendingMessage(offset int64, msg *commitlog.Message) {
	ack := &client.Ack{
		Stream:             p.Stream,
		PartitionSubject:   p.Subject,
		MsgSubject:         string(msg.Headers["subject"]),
		Offset:             offset,
		AckInbox:           msg.AckInbox,
		CorrelationId:      msg.CorrelationID,
		AckPolicy:          msg.AckPolicy,
		ReceptionTimestamp: msg.Timestamp,
	}
	if msg.AckPolicy == client.AckPolicy_LEADER {
		// Send the ack now since AckPolicy_LEADER means we ack as soon as the
		// leader has written the message to its WAL.
		p.sendAck(ack)
	}

	// Fast path: skip commit queue for RF=1 when ack policy doesn't require
	// waiting for replication (LEADER or NONE). The ack is already sent above
	// for LEADER, and NONE doesn't need any ack. The commit queue is only
	// needed for AckPolicy_ALL which requires waiting for ISR replication.
	// Note: High watermark is updated once per batch in messageProcessingLoop,
	// not per message, to avoid contention.
	if p.ReplicationFactor == 1 && msg.AckPolicy != client.AckPolicy_ALL {
		return
	}

	if err := p.commitQueue.Put(ack); err != nil {
		// An error here indicates the queue was disposed as a result of the
		// leader stepping down.
		p.srv.logger.Errorf("Failed to add message to commit queue for partition %s: %v", p, err)
	}
}

// startReplicating starts a long-running goroutine which handles committing
// messages in the commit queue and a replication goroutine for each replica.
func (p *partition) startReplicating(epoch uint64, stop chan struct{}) {
	if p.ReplicationFactor > 1 {
		p.srv.logger.Debugf("Replicating partition %s to followers", p)
	}
	p.commitQueue = queue.New(100)
	p.srv.startGoroutineWG(func() {
		p.commitLoop(stop)
	}, p.shutdown)

	p.replicators = make(map[string]*replicator, len(p.replicas)-1)
	for replica := range p.replicas {
		if replica == p.srv.config.Clustering.ServerID {
			// Don't replicate to ourselves.
			continue
		}
		r := newReplicator(epoch, replica, p)
		p.replicators[replica] = r
		p.srv.api.startGoroutineWithArgsWG(func(args ...interface{}) {
			args[0].(*replicator).start(stop)
		}, p.shutdown, r)
	}
}

// commitLoop is a long-running loop which checks to see if messages in the
// commit queue can be committed and, if so, removes them from the queue and
// sends client acks. It runs until the stop channel is closed.
func (p *partition) commitLoop(stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-p.commitCheck:
		}

		p.mu.RLock()

		// Check if the ISR size is below the minimum ISR size. If it is, we
		// cannot commit any messages.
		isrSize := len(p.isr)
		if isrSize < p.minISR {
			p.mu.RUnlock()
			p.srv.logger.Errorf(
				"Unable to commit messages for partition %s, ISR size (%d) below minimum (%d)",
				p, isrSize, p.minISR)
			continue
		}

		// Commit all messages in the queue that have been replicated by all
		// replicas in the ISR. Do this by taking the min of all latest offsets
		// in the ISR, updating the HW, and acking queue entries.
		var (
			latestOffsets = make([]int64, isrSize)
			i             = 0
		)
		for _, replica := range p.isr {
			latestOffsets[i] = replica.getLatestOffset()
			i++
		}
		p.mu.RUnlock()
		var (
			minLatest      = min(latestOffsets)
			committed, err = p.commitQueue.TakeUntil(func(pending interface{}) bool {
				return pending.(*client.Ack).Offset <= minLatest
			})
		)

		p.log.SetHighWatermark(minLatest)

		// An error here indicates the queue was disposed as a result of the
		// leader stepping down.
		if err != nil {
			return
		}

		if len(committed) == 0 {
			continue
		}

		// Ack any committed entries (if applicable).
		for _, ackIface := range committed {
			ack := ackIface.(*client.Ack)
			// Only send an ack if the AckPolicy is ALL.
			if ack.AckPolicy == client.AckPolicy_ALL {
				p.sendAck(ack)
			}
		}
	}
}

// sendAck publishes an ack to the specified AckInbox. If no AckInbox is set,
// this does nothing.
func (p *partition) sendAck(ack *client.Ack) {
	if ack.AckInbox == "" {
		return
	}
	ack.CommitTimestamp = timestamp()
	data, err := proto.MarshalAck(ack)
	if err != nil {
		panic(err)
	}
	if err := p.srv.ncAcks.Publish(ack.AckInbox, data); err != nil {
		p.srv.logger.Errorf("Error sending ack for partition %s: %v", p, err)
	}
}

// sendTooLargeNack publishes an ack containing an error indicating the message
// exceeded the max replication size to the specified AckInbox. If no AckInbox
// is set, this does nothing.
func (p *partition) sendTooLargeNack(msg *commitlog.Message) {
	p.srv.logger.Errorf(
		"Rejecting message received on partition %s that exceeds clustering.replication.max.bytes (%d)",
		p, p.srv.config.Clustering.ReplicationMaxBytes)
	if msg.AckInbox == "" {
		return
	}
	ack := &client.Ack{
		Stream:             p.Stream,
		PartitionSubject:   p.Subject,
		MsgSubject:         string(msg.Headers["subject"]),
		AckInbox:           msg.AckInbox,
		CorrelationId:      msg.CorrelationID,
		AckPolicy:          msg.AckPolicy,
		ReceptionTimestamp: msg.Timestamp,
		AckError:           client.Ack_TOO_LARGE,
	}
	data, err := proto.MarshalAck(ack)
	if err != nil {
		panic(err)
	}
	if err := p.srv.ncAcks.Publish(ack.AckInbox, data); err != nil {
		p.srv.logger.Errorf("Error sending ack for partition %s: %v", p, err)
	}
}

// replicationRequestLoop is a long-running loop which sends replication
// requests to the partition leader, handles replicating messages, and checks
// the health of the leader.
func (p *partition) replicationRequestLoop(leader string, epoch uint64, stop <-chan struct{}) {
	leaderLastSeen := time.Now()
	for {
		select {
		case <-stop:
			return
		default:
		}

		replicated, err := p.sendReplicationRequest(epoch)
		if err != nil {
			p.srv.logger.Errorf(
				"Error sending replication request for partition %s: %v", p, err)

			// Check if the loop has since been stopped. This is possible, for
			// example, if another leader was since elected.
			select {
			case <-stop:
				return
			default:
			}
		} else {
			leaderLastSeen = time.Now()
		}

		// Check if leader has exceeded max leader timeout.
		p.checkLeaderHealth(leader, epoch, leaderLastSeen)

		// If there is more data, continue replicating.
		if replicated > 0 {
			continue
		}

		// If we are caught up with the leader, wait for data.
		wait := p.computeReplicaFetchSleep()
		select {
		case <-stop:
			return
		case <-time.After(wait):
			// Check in with leader to maintain health status.
			continue
		case <-p.notify:
			// Leader has signalled more data is available.
			continue
		}
	}
}

// checkLeaderHealth checks if the leader has responded within
// ReplicaMaxLeaderTimeout and, if not, reports the leader to the controller.
func (p *partition) checkLeaderHealth(leader string, epoch uint64, leaderLastSeen time.Time) {
	lastSeenElapsed := time.Since(leaderLastSeen)
	if lastSeenElapsed > p.srv.config.Clustering.ReplicaMaxLeaderTimeout {
		// Leader has not sent a response in ReplicaMaxLeaderTimeout, so report
		// it to controller.
		p.srv.logger.Errorf("Leader %s for partition %s exceeded max leader timeout "+
			"(last seen: %s), reporting leader to controller",
			leader, p, lastSeenElapsed)
		req := &proto.ReportLeaderOp{
			Stream:      p.Stream,
			Partition:   p.Id,
			Replica:     p.srv.config.Clustering.ServerID,
			Leader:      leader,
			LeaderEpoch: epoch,
		}
		if err := p.srv.metadata.ReportLeader(context.Background(), req); err != nil {
			p.srv.logger.Errorf("Failed to report leader %s for partition %s: %s",
				leader, p, err.Err())
		}
	}
}

// computeReplicaFetchSleep calculates the time to backoff before sending
// another replication request.
func (p *partition) computeReplicaFetchSleep() time.Duration {
	sleep := p.srv.config.Clustering.ReplicaMaxIdleWait
	// Subtract some random jitter from the max wait time.
	return sleep - time.Duration(rand.Intn(2000))*time.Millisecond
}

// sendReplicationRequest sends a replication request to the partition leader
// and processes the response. It returns an int indicating the number of
// messages that were replicated. Zero (without an error) indicates the
// follower is caught up with the leader.
func (p *partition) sendReplicationRequest(leaderEpoch uint64) (int, error) {
	data, err := proto.MarshalReplicationRequest(&proto.ReplicationRequest{
		ReplicaID:   p.srv.config.Clustering.ServerID,
		Offset:      p.log.NewestOffset(),
		LeaderEpoch: leaderEpoch,
	})
	if err != nil {
		panic(err)
	}
	resp, err := p.srv.ncRepl.Request(
		p.getReplicationRequestInbox(),
		data,
		p.srv.config.Clustering.ReplicaFetchTimeout,
	)
	if err != nil {
		return 0, err
	}
	return p.handleReplicationResponse(resp), nil
}

// truncateUncommitted truncates the log up to the start offset of the first
// leader epoch larger than the current epoch. This removes any potentially
// uncommitted messages in the log.
func (p *partition) truncateUncommitted() error {
	// Request the last offset for the epoch from the leader.
	var (
		lastOffset  int64
		err         error
		leaderEpoch = p.log.LastLeaderEpoch()
	)
	for i := 0; i < 3; i++ {
		lastOffset, err = p.sendLeaderOffsetRequest(leaderEpoch)
		// Retry timeouts.
		if err == nats.ErrTimeout {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	if err != nil {
		p.srv.logger.Errorf(
			"Failed to fetch last offset for leader epoch for partition %s: %v",
			p, err)
		// Fall back to HW truncation if we fail to fetch last offset for
		// leader epoch.
		// TODO: Should this be configurable since there is potential for data
		// loss or replica divergence?
		return p.truncateToHW()
	}

	p.srv.logger.Debugf("Truncating log for partition %s to %d", p, lastOffset)
	// Add 1 because we don't want to truncate the last offset itself.
	return p.log.Truncate(lastOffset + 1)
}

// sendLeaderOffsetRequest sends a request to the leader for the last offset
// for the current leader epoch.
func (p *partition) sendLeaderOffsetRequest(leaderEpoch uint64) (int64, error) {
	data, err := proto.MarshalLeaderEpochOffsetRequest(
		&proto.LeaderEpochOffsetRequest{LeaderEpoch: leaderEpoch})
	if err != nil {
		panic(err)
	}
	resp, err := p.srv.ncRepl.Request(
		p.getLeaderOffsetRequestInbox(),
		data,
		time.Second,
	)
	if err != nil {
		return 0, err
	}
	offsetResp, err := proto.UnmarshalLeaderEpochOffsetResponse(resp.Data)
	if err != nil {
		return 0, err
	}
	return offsetResp.EndOffset, nil
}

// truncateToHW truncates the log up to the latest high watermark. This removes
// any potentially uncommitted messages in the log. However, this should only
// be used as a fallback in the event that epoch-based truncation fails. There
// are a couple edge cases with this method of truncating the log that could
// result in data loss or replica divergence (see issue #38).
func (p *partition) truncateToHW() error {
	var (
		newestOffset = p.log.NewestOffset()
		hw           = p.log.HighWatermark()
	)
	if newestOffset == hw {
		return nil
	}
	p.srv.logger.Debugf("Truncating log for partition %s to HW %d", p, hw)
	// Add 1 because we don't want to truncate the HW itself.
	return p.log.Truncate(hw + 1)
}

// inISR indicates if the given replica is in the current in-sync replicas set.
func (p *partition) inISR(replica string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.isr[replica]
	return ok
}

// inReplicas indicates if the given broker is a replica for the partition.
func (p *partition) inReplicas(id string) bool {
	_, ok := p.replicas[id]
	return ok
}

// RemoveFromISR removes the given replica from the in-sync replicas set. It
// returns an error if the broker is not a partition replica. This will also
// insert a check to see if pending messages need to be committed since the ISR
// shrank.
func (p *partition) RemoveFromISR(replica string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.inReplicas(replica) {
		return fmt.Errorf("%s not a replica", replica)
	}
	delete(p.isr, replica)

	// Also update the ISR on the protobuf so this state is persisted.
	p.Isr = make([]string, 0, len(p.isr))
	for replica := range p.isr {
		p.Isr = append(p.Isr, replica)
	}

	// Check if ISR went below minimum ISR size. This is important for
	// operators to be aware of.
	isrSize := len(p.isr)
	if !p.belowMinISR && isrSize < p.minISR {
		p.srv.logger.Errorf("ISR for partition %s has shrunk below minimum size %d, currently %d",
			p, p.minISR, isrSize)
		p.belowMinISR = true
	}

	// We may need to commit messages since the ISR shrank.
	if p.isLeading {
		select {
		case p.commitCheck <- struct{}{}:
		default:
		}
	}

	return nil
}

// AddToISR adds the given replica to the in-sync replicas set. It returns an
// error if the broker is not a partition replica.
func (p *partition) AddToISR(rep string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.inReplicas(rep) {
		return fmt.Errorf("%s not a replica", rep)
	}
	p.isr[rep] = &replica{offset: -1}

	// Also update the ISR on the protobuf so this state is persisted.
	p.Isr = make([]string, 0, len(p.isr))
	for replica := range p.isr {
		p.Isr = append(p.Isr, replica)
	}

	// Check if ISR recovered from being below the minimum ISR size.
	isrSize := len(p.isr)
	if p.belowMinISR && isrSize >= p.minISR {
		p.srv.logger.Infof("ISR for partition %s has recovered from being below minimum size %d, currently %d",
			p, p.minISR, isrSize)
		p.belowMinISR = false
	}

	return nil
}

// GetEpoch returns the current partition epoch. The epoch is a monotonically
// increasing number which increases when a change is made to the partition. This
// is used to determine if an operation is outdated.
func (p *partition) GetEpoch() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Epoch
}

// SetEpoch sets the current partition epoch. See GetEpoch for information on the
// epoch's purpose.
func (p *partition) SetEpoch(epoch uint64) {
	p.mu.Lock()
	p.Epoch = epoch
	p.mu.Unlock()
}

// Marshal serializes the partition into a byte slice.
func (p *partition) Marshal() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()
	data, err := p.Partition.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

// ISRSize returns the current number of replicas in the in-sync replicas set.
func (p *partition) ISRSize() int {
	p.mu.RLock()
	size := len(p.isr)
	p.mu.RUnlock()
	return size
}

// GetISR returns the in-sync replicas set.
func (p *partition) GetISR() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	isr := make([]string, 0, len(p.isr))
	for replica := range p.isr {
		isr = append(isr, replica)
	}
	return isr
}

// GetReplicas returns the list of all brokers which are replicas for the
// partition.
func (p *partition) GetReplicas() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	replicas := make([]string, 0, len(p.replicas))
	for replica := range p.replicas {
		replicas = append(replicas, replica)
	}
	return replicas
}

// updateISRLatestOffset updates the given replica's latest log offset. When a
// replica's latest log offset increases, we check to see if anything in the
// commit queue can be committed.
func (p *partition) updateISRLatestOffset(replica string, offset int64) {
	p.mu.RLock()
	rep, ok := p.isr[replica]
	p.mu.RUnlock()
	if !ok {
		// Replica is not currently in ISR.
		return
	}
	if rep.updateLatestOffset(offset) {
		// If offset updated, we may need to commit messages.
		select {
		case p.commitCheck <- struct{}{}:
		default:
		}
	}
}

// sendPartitionNotification sends a message to the given partition replica to
// indicate new data is available in the log.
func (p *partition) sendPartitionNotification(replica string) {
	req, err := proto.MarshalPartitionNotification(&proto.PartitionNotification{
		Stream:    p.Stream,
		Partition: p.Id,
	})
	if err != nil {
		panic(err)
	}
	if err := p.srv.ncRepl.Publish(p.srv.getPartitionNotificationInbox(replica), req); err != nil {
		p.srv.logger.Errorf("Error sending new data notification to replica %s for partition %s: %v",
			replica, p, err)
	}
}

// pauseReplication stops replication on the leader. This is for unit testing
// purposes only.
func (p *partition) pauseReplication() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pause = true
}

// getSubject returns the derived NATS subject the partition should subscribe
// to. A partitioned stream maps to separate NATS subjects: subject, subject.1,
// subject.2, etc.
func (p *partition) getSubject() string {
	if p.Id == 0 {
		return p.Subject
	}
	return fmt.Sprintf("%s.%d", p.Subject, p.Id)
}

// getMessage converts the given payload into a client Message if it is one.
// This is indicated by the presence of the envelope magic number. If it is
// not, nil is returned.
func getMessage(data []byte) *client.Message {
	msg, err := proto.UnmarshalPublish(data)
	if err != nil {
		return nil
	}
	return msg
}

// natsToProtoMessage converts the given NATS message to a commit log Message.
func natsToProtoMessage(msg *nats.Msg, leaderEpoch uint64) *commitlog.Message {
	message := getMessage(msg.Data)
	m := &commitlog.Message{
		MagicByte:   1,
		Timestamp:   timestamp(),
		LeaderEpoch: leaderEpoch,
		Headers:     make(map[string][]byte),
	}

	if message != nil {
		m.Key = message.Key
		m.Value = message.Value
		for key, value := range message.Headers {
			m.Headers[key] = value
		}
		m.AckInbox = message.AckInbox
		m.CorrelationID = message.CorrelationId
		m.AckPolicy = message.AckPolicy
		m.Offset = message.Offset
	} else {
		m.Value = msg.Data
	}
	m.Headers["subject"] = []byte(msg.Subject)
	m.Headers["reply"] = []byte(msg.Reply)
	return m
}

// computeTick calculates a generic amount of time a loop should sleep before
// performing an action. This is adjusted based on how much time has elapsed
// since an arbitrary event.
func computeTick(timeElapsed, maxSleep time.Duration) time.Duration {
	tick := maxSleep - timeElapsed
	if tick < 0 {
		tick = maxSleep
	}
	return tick
}

// min returns the minimum int64 contained in the slice.
func min(v []int64) (m int64) {
	if len(v) > 0 {
		m = v[0]
	}
	for i := 1; i < len(v); i++ {
		if v[i] < m {
			m = v[i]
		}
	}
	return
}
