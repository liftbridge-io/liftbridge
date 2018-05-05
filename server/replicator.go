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
	cancelReplication func()
}

func (r *replicator) start() {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
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

		// Check if we're caught up.
		if req.HighWatermark >= r.stream.log.NewestOffset() {
			r.lastCaughtUp = now
			r.mu.Unlock()
			continue
		}

		// Cancel previous replication.
		r.cancelReplication()

		ctx, cancel := context.WithCancel(context.Background())
		r.cancelReplication = cancel
		reader, err := r.stream.log.NewReaderContext(ctx, req.HighWatermark+1)
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
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	close(r.requests)
	r.mu.Unlock()
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
		r.mu.RLock()
		if !r.running {
			r.mu.RUnlock()
			return
		}
		r.mu.RUnlock()

		now := <-ticker.C
		r.mu.RLock()
		var (
			lastSeenElapsed     = now.Sub(r.lastSeen)
			lastCaughtUpElapsed = now.Sub(r.lastCaughtUp)
		)
		r.mu.RUnlock()
		if (lastSeenElapsed > r.maxLagTime || lastCaughtUpElapsed > r.maxLagTime) && r.stream.inISR(r.replica) {
			// Follower has not sent a request or has not caught up in
			// maxLagTime, so remove it from the ISR.
			req := &proto.ShrinkISROp{
				Stream:          r.stream.Stream,
				ReplicaToRemove: r.replica,
			}
			// TODO: set timeout on context.
			if err := r.stream.srv.metadata.ShrinkISR(context.TODO(), req); err != nil {
				r.stream.srv.logger.Errorf(
					"Failed to remove replica %s for stream %s from ISR after exceeding max lag time: %v",
					r.replica, r.stream, err.Err())
			} else {
				r.stream.srv.logger.Warnf(
					"Removed replica %s for stream %s from ISR because it exceeded max lag time",
					r.replica, r.stream)
			}

			if lastSeenElapsed > r.maxLagTime {
				// If the follower has not sent a request in maxLagTime, stop
				// replicating.
				r.mu.Lock()
				r.cancelReplication()
				r.mu.Unlock()
			}
		}
	}
}

func (r *replicator) replicate(reader io.Reader, inbox string) {
	headersBuf := make([]byte, 12)
	for {
		buf, _, err := consumeStreamMessageSet(reader, headersBuf)
		if err == io.EOF {
			return
		}
		if err != nil {
			r.stream.srv.logger.Errorf("Failed to read stream message while replicating: %v", err)
			return
		}
		r.stream.srv.ncRepl.Publish(inbox, buf)
	}
}
