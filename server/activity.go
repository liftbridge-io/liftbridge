package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	lift "github.com/liftbridge-io/go-liftbridge"
	client "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/pkg/errors"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

const maxActivityPublishBackoff = 10 * time.Second

// activityManager ensures that activity events get published to the activity
// stream. This ensures that events are published at least once and in the
// order in which they occur with respect to the Raft log.
type activityManager struct {
	*Server
	lastPublishedRaftIndex uint64
	client                 lift.Client
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

// Close the activity manager and any resources associated with it.
func (a *activityManager) Close() error {
	if a.client != nil {
		if err := a.client.Close(); err != nil {
			return err
		}
	}
	return nil
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
// metadata leader. This will create a loopback Liftbridge client and set up
// the activity stream if it's enabled. It will then reconcile the last
// published event with the Raft log and begin publishing any un-published
// events. This should be called on the same goroutine as BecomeFollower.
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
// This will close the loopback Liftbridge client if it exists. This should be
// called on the same goroutine as BecomeLeader.
func (a *activityManager) BecomeFollower() error {
	if !a.config.ActivityStream.Enabled {
		return nil
	}
	if err := a.Close(); err != nil {
		return err
	}

	close(a.leadershipLostCh)
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
	var event *client.ActivityStreamEvent
	switch log.Op {
	case proto.Op_CREATE_STREAM:
		partitions := make([]int32, len(log.CreateStreamOp.Stream.Partitions))
		for i, partition := range log.CreateStreamOp.Stream.Partitions {
			partitions[i] = partition.Id
		}
		event = &client.ActivityStreamEvent{
			Op: client.ActivityStreamOp_CREATE_STREAM,
			CreateStreamOp: &client.CreateStreamOp{
				Stream:     log.CreateStreamOp.Stream.Name,
				Partitions: partitions,
			},
		}
	case proto.Op_DELETE_STREAM:
		event = &client.ActivityStreamEvent{
			Op: client.ActivityStreamOp_DELETE_STREAM,
			DeleteStreamOp: &client.DeleteStreamOp{
				Stream: log.DeleteStreamOp.Stream,
			},
		}
	case proto.Op_PAUSE_STREAM:
		event = &client.ActivityStreamEvent{
			Op: client.ActivityStreamOp_PAUSE_STREAM,
			PauseStreamOp: &client.PauseStreamOp{
				Stream:     log.PauseStreamOp.Stream,
				Partitions: log.PauseStreamOp.Partitions,
				ResumeAll:  log.PauseStreamOp.ResumeAll,
			},
		}
	case proto.Op_RESUME_STREAM:
		event = &client.ActivityStreamEvent{
			Op: client.ActivityStreamOp_RESUME_STREAM,
			ResumeStreamOp: &client.ResumeStreamOp{
				Stream:     log.ResumeStreamOp.Stream,
				Partitions: log.ResumeStreamOp.Partitions,
			},
		}
	case proto.Op_SET_STREAM_READONLY:
		event = &client.ActivityStreamEvent{
			Op: client.ActivityStreamOp_SET_STREAM_READONLY,
			SetStreamReadonlyOp: &client.SetStreamReadonlyOp{
				Stream:     log.SetStreamReadonlyOp.Stream,
				Partitions: log.SetStreamReadonlyOp.Partitions,
				Readonly:   log.SetStreamReadonlyOp.Readonly,
			},
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
	// Connect a local client that will be used to publish on the activity
	// stream.
	listenAddr := a.config.GetListenAddress()
	var err error
	a.client, err = lift.Connect([]string{fmt.Sprintf("%s:%d", listenAddr.Host, a.port)})
	if err != nil {
		return errors.Wrap(err, "failed to connect the activity stream client")
	}

	err = a.client.CreateStream(context.Background(),
		a.getActivityStreamSubject(),
		activityStream,
		lift.MaxReplication(),
	)
	if err != nil && err != lift.ErrStreamExists {
		return errors.Wrap(err, "failed to create an activity stream")
	}

	return nil
}

// publishActivityEvent publishes an event on the activity stream.
func (a *activityManager) publishActivityEvent(event *client.ActivityStreamEvent) error {
	data, err := event.Marshal()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.config.ActivityStream.PublishTimeout)
	defer cancel()

	var messageOption lift.MessageOption
	ackPolicy := a.config.ActivityStream.PublishAckPolicy
	switch ackPolicy {
	case client.AckPolicy_LEADER:
		messageOption = lift.AckPolicyLeader()
	case client.AckPolicy_ALL:
		messageOption = lift.AckPolicyAll()
	case client.AckPolicy_NONE:
		messageOption = lift.AckPolicyNone()
	default:
		panic(fmt.Sprintf("Unknown ack policy: %v", ackPolicy))
	}

	_, err = a.client.Publish(
		ctx,
		activityStream,
		data,
		messageOption,
	)
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
