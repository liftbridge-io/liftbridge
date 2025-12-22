package server

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	client "github.com/liftbridge-io/liftbridge-api/v2/go"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	pb "google.golang.org/protobuf/proto"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

const maxActivityPublishBackoff = 10 * time.Second

// activityManager ensures that activity events get published to the activity
// stream. This ensures that events are published at least once and in the
// order in which they occur with respect to the Raft log.
type activityManager struct {
	*Server
	lastPublishedRaftIndex uint64
	commitCh               chan struct{}
	leadershipLostCh       chan struct{}
	mu                     sync.RWMutex
}

func newActivityManager(s *Server) *activityManager {
	return &activityManager{
		Server:   s,
		commitCh: make(chan struct{}, 1),
	}
}

// SetLastPublishedRaftIndex sets the Raft index of the latest event published
// to the activity stream. This is used to determine where to begin publishing
// events from in the log in the case of failovers or restarts.
func (a *activityManager) SetLastPublishedRaftIndex(index uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastPublishedRaftIndex = index
}

// LastPublishedRaftIndex returns the Raft index of the latest event published
// to the activity stream. This is used to determine where to begin publishing
// events from in the log in the case of failovers or restarts.
func (a *activityManager) LastPublishedRaftIndex() uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastPublishedRaftIndex
}

// SignalCommit indicates a new event was committed to the Raft log.
func (a *activityManager) SignalCommit() {
	select {
	case a.commitCh <- struct{}{}:
	default:
	}
}

// BecomeLeader should be called when this node has been elected as the
// metadata leader. This will set up the activity stream if it's enabled. It
// will then reconcile the last published event with the Raft log and begin
// publishing any un-published events. This should be called on the same
// goroutine as BecomeFollower.
func (a *activityManager) BecomeLeader() error {
	if !a.config.ActivityStream.Enabled {
		return nil
	}
	if err := a.createActivityStream(); err != nil {
		return err
	}
	a.leadershipLostCh = make(chan struct{})
	a.startGoroutine(a.dispatch)
	return nil
}

// BecomeFollower should be called when this node has lost metadata leadership.
// This should be called on the same goroutine as BecomeLeader.
func (a *activityManager) BecomeFollower() error {
	if !a.config.ActivityStream.Enabled {
		return nil
	}

	if a.leadershipLostCh != nil {
		close(a.leadershipLostCh)
	}
	return nil
}

// dispatch is a long-running goroutine that runs while the server is the
// metadata leader. It handles publishing events to the activity stream as they
// are committed to the Raft log. Events are always published in the order in
// which they were committed to the log.
func (a *activityManager) dispatch() {
	var (
		raftNode = a.getRaft()
		index    = a.LastPublishedRaftIndex() + 1
	)
	for {
		select {
		case <-a.leadershipLostCh:
			return
		default:
		}

		// TODO: Should we instead pass the commit index from FSM apply?
		if index > raftNode.getCommitIndex() {
			// We are caught up with the Raft log, so wait for new commits.
			select {
			case <-a.commitCh:
				continue
			case <-a.leadershipLostCh:
				return
			case <-a.shutdownCh:
				return
			}
		}
		log := new(raft.Log)
		if err := raftNode.store.GetLog(index, log); err != nil {
			panic(err)
		}
		if log.Type != raft.LogCommand {
			index++
			continue
		}

		var backoff time.Duration
	RETRY:
		if err := a.handleRaftLog(log); err != nil {
			a.logger.Errorf("Failed to publish activity event: %v", err)
			backoff = computeActivityPublishBackoff(backoff)
			select {
			case <-time.After(backoff):
				goto RETRY
			case <-a.leadershipLostCh:
				return
			case <-a.shutdownCh:
				return
			}
		}
		index++
	}
}

// handleRaftLog unmarshals the Raft log into an operation and, if applicable,
// publishes an event to the activity stream.
func (a *activityManager) handleRaftLog(l *raft.Log) error {
	log := new(proto.RaftLog)
	if err := log.Unmarshal(l.Data); err != nil {
		panic(err)
	}
	event := new(client.ActivityStreamEvent)
	switch log.Op {
	case proto.Op_CREATE_STREAM:
		partitions := make([]int32, len(log.CreateStreamOp.Stream.Partitions))
		for i, partition := range log.CreateStreamOp.Stream.Partitions {
			partitions[i] = partition.Id
		}
		event.Op = client.ActivityStreamOp_CREATE_STREAM
		event.CreateStreamOp = &client.CreateStreamOp{
			Stream:     log.CreateStreamOp.Stream.Name,
			Partitions: partitions,
		}
	case proto.Op_DELETE_STREAM:
		event.Op = client.ActivityStreamOp_DELETE_STREAM
		event.DeleteStreamOp = &client.DeleteStreamOp{
			Stream: log.DeleteStreamOp.Stream,
		}
	case proto.Op_PAUSE_STREAM:
		event.Op = client.ActivityStreamOp_PAUSE_STREAM
		event.PauseStreamOp = &client.PauseStreamOp{
			Stream:     log.PauseStreamOp.Stream,
			Partitions: log.PauseStreamOp.Partitions,
			ResumeAll:  log.PauseStreamOp.ResumeAll,
		}
	case proto.Op_RESUME_STREAM:
		event.Op = client.ActivityStreamOp_RESUME_STREAM
		event.ResumeStreamOp = &client.ResumeStreamOp{
			Stream:     log.ResumeStreamOp.Stream,
			Partitions: log.ResumeStreamOp.Partitions,
		}
	case proto.Op_SET_STREAM_READONLY:
		event.Op = client.ActivityStreamOp_SET_STREAM_READONLY
		event.SetStreamReadonlyOp = &client.SetStreamReadonlyOp{
			Stream:     log.SetStreamReadonlyOp.Stream,
			Partitions: log.SetStreamReadonlyOp.Partitions,
			Readonly:   log.SetStreamReadonlyOp.Readonly,
		}
	case proto.Op_CREATE_CONSUMER_GROUP:
		// Members on create should always contain a single consumer.
		members := log.CreateConsumerGroupOp.ConsumerGroup.Members
		if len(members) == 0 {
			return nil
		}
		// Treat this as a join since it bootstraps the group.
		event.Op = client.ActivityStreamOp_JOIN_CONSUMER_GROUP
		event.JoinConsumerGroupOp = &client.JoinConsumerGroupOp{
			GroupId:    log.CreateConsumerGroupOp.ConsumerGroup.Id,
			ConsumerId: members[0].Id,
			Streams:    members[0].Streams,
		}
	case proto.Op_JOIN_CONSUMER_GROUP:
		event.Op = client.ActivityStreamOp_JOIN_CONSUMER_GROUP
		event.JoinConsumerGroupOp = &client.JoinConsumerGroupOp{
			GroupId:    log.JoinConsumerGroupOp.GroupId,
			ConsumerId: log.JoinConsumerGroupOp.ConsumerId,
			Streams:    log.JoinConsumerGroupOp.Streams,
		}
	case proto.Op_LEAVE_CONSUMER_GROUP:
		event.Op = client.ActivityStreamOp_LEAVE_CONSUMER_GROUP
		event.LeaveConsumerGroupOp = &client.LeaveConsumerGroupOp{
			GroupId:    log.LeaveConsumerGroupOp.GroupId,
			ConsumerId: log.LeaveConsumerGroupOp.ConsumerId,
			Expired:    log.LeaveConsumerGroupOp.Expired,
		}
	default:
		return nil
	}
	event.Id = l.Index
	return a.publishActivityEvent(event)
}

// createActivityStream creates the activity stream and connects a local client
// that will be subscribed to it.
func (a *activityManager) createActivityStream() error {
	status := a.metadata.CreateStream(context.Background(), &proto.CreateStreamOp{
		Stream: &proto.Stream{
			Name:    activityStream,
			Subject: a.getActivityStreamSubject(),
			Partitions: []*proto.Partition{
				{
					Stream:            activityStream,
					Subject:           a.getActivityStreamSubject(),
					ReplicationFactor: -1,
					Id:                0,
				},
			},
		},
	})
	if status == nil {
		return nil
	}
	if status.Code() != codes.AlreadyExists {
		return errors.Wrap(status.Err(), "failed to create activity stream")
	}

	return nil
}

// publishActivityEvent publishes an event on the activity stream.
func (a *activityManager) publishActivityEvent(event *client.ActivityStreamEvent) error {
	data, err := pb.Marshal(event)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.config.ActivityStream.PublishTimeout)
	defer cancel()

	_, err = a.api.Publish(ctx, &client.PublishRequest{
		Value:     data,
		Stream:    activityStream,
		AckPolicy: a.config.ActivityStream.PublishAckPolicy,
	})
	if err != nil {
		return errors.Wrap(err, "failed to publish event to stream")
	}

	a.logger.Debugf("Published %s event to activity stream", event.Op)

	// Update last published index in Raft.
	op := &proto.RaftLog{
		Op: proto.Op_PUBLISH_ACTIVITY,
		PublishActivityOp: &proto.PublishActivityOp{
			RaftIndex: event.Id,
		},
	}
	future, err := a.getRaft().applyOperation(ctx, op, nil)
	if err == nil {
		err = future.Error()
	}
	return errors.Wrap(err, "failed to update Raft")
}

func computeActivityPublishBackoff(previousBackoff time.Duration) time.Duration {
	if previousBackoff == 0 {
		return time.Second
	}
	backoff := previousBackoff * 2
	if backoff > maxActivityPublishBackoff {
		backoff = maxActivityPublishBackoff
	}
	return backoff
}
