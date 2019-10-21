package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/liftbridge-io/liftbridge/server/commitlog"
	"github.com/liftbridge-io/liftbridge/server/proto"
)

const (
	// replicationMaxSize is the max payload size to send in replication
	// messages to followers. The default NATS max message size is 1MB, so
	// we'll use that.
	replicationMaxSize = 1024 * 1024

	// replicationOverhead is the non-data size overhead of replication
	// messages: 8 bytes for the leader epoch and 8 bytes for the HW.
	replicationOverhead = 16
)

// replicationRequest wraps a ReplicationRequest protobuf and a NATS subject
// where responses should be sent.
type replicationRequest struct {
	*proto.ReplicationRequest
	request  *nats.Msg
	received time.Time
}

// replicator handles replication requests from a particular replica and tracks
// its health. Requests are received on the requests channel and a long-running
// loop processes them and sends responses. If the replica does not catch up to
// the leader's log in maxLagTime, it's removed from the ISR until it catches
// back up.
type replicator struct {
	partition    *partition
	replica      string
	maxLagTime   time.Duration
	lastCaughtUp time.Time
	lastSeen     time.Time
	requests     chan replicationRequest
	mu           sync.RWMutex
	leader       string
	epoch        uint64
	headersBuf   [28]byte // scratch buffer for reading message headers
	writer       replicationProtocolWriter
	waiter       <-chan struct{}
}

func newReplicator(epoch uint64, replica string, p *partition) *replicator {
	return &replicator{
		epoch:      epoch,
		replica:    replica,
		partition:  p,
		requests:   make(chan replicationRequest, 1),
		maxLagTime: p.srv.config.Clustering.ReplicaMaxLagTime,
		leader:     p.srv.config.Clustering.ServerID,
	}
}

// start a long-running replication loop for the given leader epoch until the
// stop channel is closed. This loop will receive messages from the requests
// channel, update the replica last-seen timestamp and latest offset, and send
// a batch of messages starting at the requested offset, if there are any
// available. The response will also include the leader epoch and HW. If the
// replica doesn't send a request or catch up to the leader's log in
// maxLagTime, it will be removed from the ISR until it catches back up.
func (r *replicator) start(stop <-chan struct{}) {
	r.mu.Lock()
	now := time.Now()
	r.lastSeen = now
	r.lastCaughtUp = now
	r.writer = newReplicationProtocolWriter(r, stop)
	r.mu.Unlock()

	// Start a goroutine to track the replica's health.
	r.partition.srv.startGoroutine(func() { r.tick(stop) })

	var req replicationRequest
	for {
		select {
		case <-stop:
			return
		case req = <-r.requests:
		}

		r.mu.Lock()
		r.lastSeen = req.received
		r.mu.Unlock()

		// Update the ISR replica's latest offset for the partition. This is
		// used by the leader to know when to commit messages.
		r.partition.updateISRLatestOffset(r.replica, req.Offset)

		var (
			latest   = r.partition.log.NewestOffset()
			earliest = r.partition.log.OldestOffset()
		)

		// Check if we're caught up.
		if req.Offset >= latest {
			r.caughtUp(stop, latest, req)
			continue
		}

		// Create a log reader starting at the requested offset.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reader, err := r.partition.log.NewReader(req.Offset+1, true)
		if err != nil {
			r.partition.srv.logger.Errorf(
				"Failed to create replication reader for partition %s "+
					"and replica %s (requested offset %d, earliest %d, latest %d): %v",
				r.partition, r.replica, req.Offset+1, earliest, latest, err)
			// Send a response to short-circuit request timeout.
			if err := r.sendHW(req.request); err != nil {
				r.partition.srv.logger.Errorf("Failed to send HW for partition %s to replica %s: %v",
					r.partition, req.ReplicaID, err)
			}
			continue
		}

		// Send a batch of messages to the replica.
		if err := r.replicate(ctx, reader, req.request, req.Offset); err != nil {
			// Send a response to short-circuit request timeout.
			if err := r.sendHW(req.request); err != nil {
				r.partition.srv.logger.Errorf("Failed to send HW for partition %s to replica %s: %v",
					r.partition, req.ReplicaID, err)
			}
			continue
		}
	}
}

func (r *replicator) request(req replicationRequest) {
	select {
	case r.requests <- req:
	default:
		r.partition.srv.logger.Warnf("Dropped replication request for partition %s from replica %s",
			r.partition, req.ReplicaID)
	}
}

// tick is a long-running call that checks to see if the follower hasn't sent
// any replication requests or hasn't consumed up to the leader's log end
// offset for the lag-time duration. If this is the case, the follower is
// removed from the ISR until it catches back up.
func (r *replicator) tick(stop <-chan struct{}) {
	ticker := time.NewTicker(r.maxLagTime)
	defer ticker.Stop()
	var now time.Time
	for {
		select {
		case <-stop:
			return
		case now = <-ticker.C:
		}
		r.mu.RLock()
		var (
			lastSeenElapsed     = now.Sub(r.lastSeen)
			lastCaughtUpElapsed = now.Sub(r.lastCaughtUp)
		)
		r.mu.RUnlock()
		outOfSync := lastSeenElapsed > r.maxLagTime || lastCaughtUpElapsed > r.maxLagTime
		if outOfSync && r.partition.inISR(r.replica) {
			// Follower has not sent a request or has not caught up in
			// maxLagTime, so remove it from the ISR.
			r.partition.srv.logger.Errorf("Replica %s for partition %s exceeded max lag time "+
				"(last seen: %s, last caught up: %s), removing from ISR",
				r.replica, r.partition, lastSeenElapsed, lastCaughtUpElapsed)

			r.shrinkISR()
		} else if !outOfSync && !r.partition.inISR(r.replica) {
			// Add replica back into ISR.
			r.partition.srv.logger.Infof("Replica %s for partition %s caught back up with leader, "+
				"rejoining ISR", r.replica, r.partition)
			r.expandISR()
		}
	}
}

// shrinkISR sends a ShrinkISR request to the controller to remove the replica
// from the ISR.
func (r *replicator) shrinkISR() {
	req := &proto.ShrinkISROp{
		Stream:          r.partition.Stream,
		Partition:       r.partition.Id,
		ReplicaToRemove: r.replica,
		Leader:          r.leader,
		LeaderEpoch:     r.epoch,
	}
	if err := r.partition.srv.metadata.ShrinkISR(context.Background(), req); err != nil {
		r.partition.srv.logger.Errorf(
			"Failed to remove replica %s for partition %s from ISR: %v",
			r.replica, r.partition, err.Err())
	}
}

// expandISR sends an ExpandISR request to the controller to add the replica to
// the ISR.
func (r *replicator) expandISR() {
	req := &proto.ExpandISROp{
		Stream:       r.partition.Stream,
		Partition:    r.partition.Id,
		ReplicaToAdd: r.replica,
		Leader:       r.leader,
		LeaderEpoch:  r.epoch,
	}
	if err := r.partition.srv.metadata.ExpandISR(context.Background(), req); err != nil {
		r.partition.srv.logger.Errorf(
			"Failed to add replica %s for partition %s to ISR: %v",
			r.replica, r.partition, err.Err())
	}
}

// replicate sends a batch of messages to the given NATS inbox along with the
// leader epoch and HW.
func (r *replicator) replicate(
	ctx context.Context, reader *commitlog.Reader, request *nats.Msg, offset int64) error {

	var (
		newestOffset = r.partition.log.NewestOffset()
		message      commitlog.Message
		err          error
	)
	for offset < newestOffset && r.writer.Len() < replicationMaxSize {
		message, offset, _, _, err = reader.ReadMessage(ctx, r.headersBuf[:])
		if err != nil {
			r.partition.srv.logger.Errorf("Failed to read message while replicating: %v", err)
			return err
		}

		// Check if this message will put us over the batch size limit. If it
		// does, flush the batch now.
		if uint32(len(message))+uint32(len(r.headersBuf))+uint32(r.writer.Len()) > replicationMaxSize {
			break
		}

		// Write the message to the buffer.
		if err := r.writer.Write(offset, r.headersBuf[:], message); err != nil {
			r.partition.srv.logger.Errorf("Failed to write message to buffer while replicating: %v", err)
			return err
		}
	}

	// Flush the batch.
	if err := r.writer.Flush(request.Respond); err != nil {
		r.partition.srv.logger.Errorf("Failed to flush buffer while replicating: %v", err)
		return err
	}
	return nil
}

// caughtUp is called when the follower has caught up with the leader's log.
// This will register a data waiter on the log so that the leader can notify
// the follower when new data is available to replicate.
func (r *replicator) caughtUp(stop <-chan struct{}, leo int64, req replicationRequest) {
	r.mu.Lock()
	r.lastCaughtUp = req.received
	waiter := r.waiter
	if waiter == nil {
		// Register a waiter to be notified when new messages are written after
		// the current log end offset to preempt an idle follower.
		waiter = r.partition.log.NotifyLEO(r, leo)
		r.partition.srv.startGoroutine(func() {
			select {
			case <-waiter:
				r.partition.sendPartitionNotification(req.ReplicaID)
				r.mu.Lock()
				r.waiter = nil
				r.mu.Unlock()
			case <-stop:
			}
		})
		r.waiter = waiter
	}
	r.mu.Unlock()

	if err := r.sendHW(req.request); err != nil {
		r.partition.srv.logger.Errorf("Failed to send HW for partition %s to replica %s: %v",
			r.partition, req.ReplicaID, err)
	}
}

// sendHW sends the leader epoch and HW to the given NATS inbox.
func (r *replicator) sendHW(request *nats.Msg) error {
	r.writer.Reset()
	return r.writer.Flush(request.Respond)
}

type replicationProtocolWriter interface {
	Write(offset int64, headers, message []byte) error
	Flush(func(data []byte) error) error
	Len() int
	Reset()
}

type protocolWriter struct {
	*replicator
	buf        *bytes.Buffer
	log        CommitLog
	lastOffset int64
	stop       <-chan struct{}
}

func newReplicationProtocolWriter(r *replicator, stop <-chan struct{}) replicationProtocolWriter {
	w := &protocolWriter{
		replicator: r,
		buf:        new(bytes.Buffer),
		log:        r.partition.log,
		stop:       stop,
	}
	w.Reset()
	return w
}

func (w *protocolWriter) Write(offset int64, headers, message []byte) error {
	if _, err := w.buf.Write(headers); err != nil {
		return err
	}
	if _, err := w.buf.Write(message); err != nil {
		return err
	}
	w.lastOffset = offset
	return nil
}

func (w *protocolWriter) Flush(write func([]byte) error) error {
	data := w.buf.Bytes()
	// Replace the HW.
	proto.Encoding.PutUint64(data[8:], uint64(w.log.HighWatermark()))

	if err := write(data); err != nil {
		w.Reset()
		return err
	}

	w.Reset()
	return nil
}

func (w *protocolWriter) Len() int {
	return w.buf.Len()
}

func (w *protocolWriter) Reset() {
	w.buf.Reset()
	w.lastOffset = -1

	// Write the leader epoch.
	binary.Write(w.buf, proto.Encoding, w.replicator.epoch)
	// Reserve space for the HW. This will be replaced with the HW at the time
	// of flush.
	binary.Write(w.buf, proto.Encoding, int64(0))
}
