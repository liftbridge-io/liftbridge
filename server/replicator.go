package server

import (
	"io"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/tylertreat/jetbridge/server/proto"
)

type replicator struct {
	stream            *stream
	replica           string
	hw                int64
	maxLagTime        time.Duration
	lastCaughtUp      time.Time
	lastSeen          time.Time
	requests          chan *proto.ReplicationRequest
	mu                sync.RWMutex
	running           bool
	cancelReplication context.CancelFunc
	leader            string
	epoch             uint64
}

func (r *replicator) start(epoch uint64) {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	r.epoch = epoch
	r.running = true
	now := time.Now()
	r.lastSeen = now
	r.lastCaughtUp = now
	r.mu.Unlock()

	go r.tick()

	for req := range r.requests {
		now := time.Now()
		r.mu.Lock()
		r.lastSeen = now

		// Update the ISR replica's latest offset for the stream. This is used
		// by the leader to know when to commit messages.
		r.stream.updateISRLatestOffset(r.replica, req.Offset)

		// Check if we're caught up.
		if req.Offset >= r.stream.log.NewestOffset() {
			r.lastCaughtUp = now
			r.mu.Unlock()
			r.sendHW(req.Inbox)
			continue
		}

		// Cancel previous replication.
		r.cancelReplication()

		ctx, cancel := context.WithCancel(context.Background())
		r.cancelReplication = cancel
		reader, err := r.stream.log.NewReaderContext(ctx, req.Offset+1)
		if err != nil {
			// TODO: if this errors, something is really screwed up. In
			// particular, it probably means the offset does not exist. We
			// could send a message back to the follower indicating this. For
			// now, log it and do nothing.
			r.stream.srv.logger.Errorf("Failed to create replicator reader for stream %s and replica %s: %v",
				r.stream, r.replica, err)
			r.mu.Unlock()
			continue
		}
		r.mu.Unlock()

		go r.replicate(reader, req.Inbox)
	}
}

func (r *replicator) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.running {
		return
	}
	r.running = false
	close(r.requests)
}

func (r *replicator) request(req *proto.ReplicationRequest) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.running {
		return
	}
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
func (r *replicator) tick() {
	ticker := time.NewTicker(r.maxLagTime)
	defer ticker.Stop()
	for {
		now := <-ticker.C
		r.mu.RLock()
		if !r.running {
			r.mu.RUnlock()
			return
		}
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

			if lastSeenElapsed > r.maxLagTime {
				// If the follower has not sent a request in maxLagTime, stop
				// replicating.
				r.mu.Lock()
				r.cancelReplication()
				r.mu.Unlock()
			}
		} else if !outOfSync && !r.stream.inISR(r.replica) {
			// Add replica back into ISR.
			r.stream.srv.logger.Warnf("Replica %s for stream %s caught back up with leader, "+
				"rejoining ISR", r.replica, r.stream)
			r.expandISR()
		}
	}
}

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

func (r *replicator) replicate(reader io.Reader, inbox string) {
	headersBuf := make([]byte, 12)
	for {
		ms, _, err := consumeStreamMessageSet(reader, headersBuf)
		if err == io.EOF {
			return
		}
		if err != nil {
			r.stream.srv.logger.Errorf("Failed to read stream message while replicating: %v", err)
			return
		}
		// Piggyback HW in the first 8 bytes.
		buf := make([]byte, 8+len(ms))
		proto.Encoding.PutUint64(buf, uint64(r.stream.log.HighWatermark()))
		copy(buf[8:], ms)
		r.stream.srv.ncRepl.Publish(inbox, buf)
	}
}

func (r *replicator) sendHW(inbox string) {
	buf := make([]byte, 8)
	proto.Encoding.PutUint64(buf, uint64(r.stream.log.HighWatermark()))
	r.stream.srv.ncRepl.Publish(inbox, buf)
}
