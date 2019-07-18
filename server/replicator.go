package server

import (
	"bytes"
	"encoding/binary"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/net/context"

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
	request *nats.Msg
}

// replicator handles replication requests from a particular replica and tracks
// its health. Requests are received on the requests channel and a long-running
// loop processes them and sends responses. If the replica does not catch up to
// the leader's log in maxLagTime, it's removed from the ISR until it catches
// back up.
type replicator struct {
	stream       *stream
	replica      string
	maxLagTime   time.Duration
	lastCaughtUp time.Time
	lastSeen     time.Time
	requests     chan replicationRequest
	mu           sync.RWMutex
	leader       string
	epoch        uint64
	headersBuf   [28]byte // scratch buffer for reading message headers
}

// start a long-running replication loop for the given leader epoch until the
// stop channel is closed. This loop will receive messages from the requests
// channel, update the replica last-seen timestamp and latest offset, and send
// a batch of messages starting at the requested offset, if there are any
// available. The response will also include the leader epoch and HW. If the
// replica doesn't send a request or catch up to the leader's log in
// maxLagTime, it will be removed from the ISR until it catches back up.
func (r *replicator) start(epoch uint64, stop chan struct{}) {
	r.mu.Lock()
	r.epoch = epoch
	now := time.Now()
	r.lastSeen = now
	r.lastCaughtUp = now
	r.mu.Unlock()

	// Start a goroutine to track the replica's health.
	r.stream.srv.startGoroutine(func() { r.tick(stop) })

	var req replicationRequest
	for {
		select {
		case <-stop:
			return
		case req = <-r.requests:
		}

		now := time.Now()
		r.mu.Lock()
		r.lastSeen = now
		r.mu.Unlock()

		// Update the ISR replica's latest offset for the stream. This is used
		// by the leader to know when to commit messages.
		r.stream.updateISRLatestOffset(r.replica, req.Offset)

		var (
			latest   = r.stream.log.NewestOffset()
			earliest = r.stream.log.OldestOffset()
		)

		// Check if we're caught up.
		if req.Offset >= latest {
			r.mu.Lock()
			r.lastCaughtUp = now
			r.mu.Unlock()
			r.sendHW(req.request)
			continue
		}

		// Create a log reader starting at the requested offset.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reader, err := r.stream.log.NewReader(req.Offset+1, true)
		if err != nil {
			r.stream.srv.logger.Errorf(
				"Failed to create replication reader for stream %s "+
					"and replica %s (requested offset %d, earliest %d, latest %d): %v",
				r.stream, r.replica, req.Offset+1, earliest, latest, err)
			// Send a response to short-circuit request timeout.
			r.sendHW(req.request)
			continue
		}

		// Send a batch of messages to the replica.
		if err := r.replicate(ctx, reader, req.request, req.Offset); err != nil {
			// Send a response to short-circuit request timeout.
			r.sendHW(req.request)
			continue
		}
	}
}

func (r *replicator) request(req replicationRequest) {
	select {
	case r.requests <- req:
	default:
		r.stream.srv.logger.Warnf("Dropped replication request for stream %s from replica %s",
			r.stream, req.ReplicaID)
	}
}

// tick is a long-running call that checks to see if the follower hasn't sent
// any replication requests or hasn't consumed up to the leader's log end
// offset for the lag-time duration. If this is the case, the follower is
// removed from the ISR until it catches back up.
func (r *replicator) tick(stop chan struct{}) {
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
		if outOfSync && r.stream.inISR(r.replica) {
			// Follower has not sent a request or has not caught up in
			// maxLagTime, so remove it from the ISR.
			r.stream.srv.logger.Errorf("Replica %s for stream %s exceeded max lag time "+
				"(last seen: %s, last caught up: %s), removing from ISR",
				r.replica, r.stream, lastSeenElapsed, lastCaughtUpElapsed)

			r.shrinkISR()
		} else if !outOfSync && !r.stream.inISR(r.replica) {
			// Add replica back into ISR.
			r.stream.srv.logger.Infof("Replica %s for stream %s caught back up with leader, "+
				"rejoining ISR", r.replica, r.stream)
			r.expandISR()
		}
	}
}

// shrinkISR sends a ShrinkISR request to the controller to remove the replica
// from the ISR.
func (r *replicator) shrinkISR() {
	req := &proto.ShrinkISROp{
		Subject:         r.stream.Subject,
		Name:            r.stream.Name,
		ReplicaToRemove: r.replica,
		Leader:          r.leader,
		LeaderEpoch:     r.epoch,
	}
	if err := r.stream.srv.metadata.ShrinkISR(context.Background(), req); err != nil {
		r.stream.srv.logger.Errorf(
			"Failed to remove replica %s for stream %s from ISR: %v",
			r.replica, r.stream, err.Err())
	}
}

// expandISR sends an ExpandISR request to the controller to add the replica to
// the ISR.
func (r *replicator) expandISR() {
	req := &proto.ExpandISROp{
		Subject:      r.stream.Subject,
		Name:         r.stream.Name,
		ReplicaToAdd: r.replica,
		Leader:       r.leader,
		LeaderEpoch:  r.epoch,
	}
	if err := r.stream.srv.metadata.ExpandISR(context.Background(), req); err != nil {
		r.stream.srv.logger.Errorf(
			"Failed to add replica %s for stream %s to ISR: %v",
			r.replica, r.stream, err.Err())
	}
}

// replicate sends a batch of messages to the given NATS inbox along with the
// leader epoch and HW.
func (r *replicator) replicate(
	ctx context.Context, reader *commitlog.Reader, request *nats.Msg, offset int64) error {

	buf := new(bytes.Buffer)
	// Write the leader epoch to the buffer.
	if err := binary.Write(buf, proto.Encoding, r.epoch); err != nil {
		r.stream.srv.logger.Errorf("Failed to write leader epoch to buffer while replicating: %v", err)
		return err
	}
	// Reserve space for the HW. This will be replaced with the HW at the time
	// of flush.
	if err := binary.Write(buf, proto.Encoding, int64(0)); err != nil {
		r.stream.srv.logger.Errorf("Failed to write HW to buffer while replicating: %v", err)
		return err
	}

	var (
		newestOffset = r.stream.log.NewestOffset()
		message      commitlog.Message
		err          error
	)
	for offset < newestOffset && buf.Len() < replicationMaxSize {
		message, offset, _, _, err = reader.ReadMessage(ctx, r.headersBuf[:])
		if err != nil {
			r.stream.srv.logger.Errorf("Failed to read message while replicating: %v", err)
			return err
		}

		// Check if this message will put us over the batch size limit. If it
		// does, flush the batch now.
		if uint32(len(message))+uint32(len(r.headersBuf))+uint32(buf.Len()) > replicationMaxSize {
			break
		}

		// Write the message to the buffer.
		if err := writeMessageToBuffer(buf, r.headersBuf[:], message); err != nil {
			r.stream.srv.logger.Errorf("Failed to write message to buffer while replicating: %v", err)
			return err
		}
	}

	// Set the HW and flush the batch.
	data := buf.Bytes()
	proto.Encoding.PutUint64(data[8:], uint64(r.stream.log.HighWatermark()))
	return request.Respond(data)
}

// writeMessageToBuffer writes the headers and message byte slices to the bytes
// buffer.
func writeMessageToBuffer(buf *bytes.Buffer, headers, message []byte) error {
	if _, err := buf.Write(headers); err != nil {
		return err
	}
	if _, err := buf.Write(message); err != nil {
		return err
	}
	return nil
}

// sendHW sends the leader epoch and HW to the given NATS inbox.
func (r *replicator) sendHW(request *nats.Msg) {
	buf := make([]byte, replicationOverhead)
	proto.Encoding.PutUint64(buf[:8], r.epoch)
	proto.Encoding.PutUint64(buf[8:], uint64(r.stream.log.HighWatermark()))
	request.Respond(buf)
}
