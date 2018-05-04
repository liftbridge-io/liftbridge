package server

import (
	"io"
	"sync"
	"time"

	"github.com/tylertreat/jetbridge/server/proto"
)

type replicator struct {
	stream       *stream
	hw           int64
	reader       io.Reader
	lastCaughtUp time.Time
	lastSeen     time.Time
	requests     chan *proto.ReplicationRequest
	mu           sync.RWMutex
	running      bool
}

func (r *replicator) start() {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.mu.Unlock()
	for req := range r.requests {
		// TODO
		println(req)
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

func (r *replicator) replicate(req *proto.ReplicationRequest) {
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
