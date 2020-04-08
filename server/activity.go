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

// activityManager ensures that activity events get published to the activity
// stream. This ensures that events are published at least once and in the
// order in which they occur.
type activityManager struct {
	*Server
	lastPublishedRaftIndex uint64
	client                 lift.Client
	commitCh               chan struct{}
	mu                     sync.RWMutex
}

func newActivityManager(s *Server) *activityManager {
	return &activityManager{
		Server:   s,
		commitCh: make(chan struct{}, 1),
	}
}

func (a *activityManager) Close() error {
	if a.client != nil {
		if err := a.client.Close(); err != nil {
			return err
		}
		a.client = nil
	}
	return nil
}

func (a *activityManager) SetLastPublishedRaftIndex(index uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastPublishedRaftIndex = index
}

func (a *activityManager) LastPublishedRaftIndex() uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastPublishedRaftIndex
}

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
	a.startGoroutine(a.catchUp)
	return nil
}

// BecomeFollower should be called when this node has lost metadata leadership.
// This will close the loopback Liftbridge client if it exists. This should be
// called on the same goroutine as BecomeLeader.
func (a *activityManager) BecomeFollower() error {
	return a.Close()
}

func (a *activityManager) catchUp() {
	var (
		raftNode = a.getRaft()
		index    = a.LastPublishedRaftIndex() + 1
	)
	for {
		// TODO: Should we instead pass the commit index from FSM apply?
		if index > raftNode.getCommitIndex() {
			// We are caught up with the Raft log, so wait for new commits.
			select {
			case <-a.commitCh:
				continue
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
	RETRY:
		if err := a.handleRaftLog(log); err != nil {
			a.logger.Errorf("Failed to publish activity event: %v", err)
			// TODO: add some kind of backoff
			select {
			case <-time.After(2 * time.Second):
				goto RETRY
			case <-a.shutdownCh:
				return
			}
		}
		index++
	}
}

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
	if err == nil {
		// Update last published index in Raft.
		op := &proto.RaftLog{
			Op: proto.Op_PUBLISH_ACTIVITY,
			PublishActivityOp: &proto.PublishActivityOp{
				RaftIndex: event.Id,
			},
		}
		future, err := a.getRaft().applyOperation(ctx, op, nil)
		if err != nil {
			return errors.Wrap(err, "failed to update Raft")
		}
		err = future.Error()
	}
	return errors.Wrap(err, "failed to publish a stream event")
}
