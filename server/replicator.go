package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/tylertreat/liftbridge/server/proto"
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

type replicator struct {
	stream             *stream
	replica            string
	hw                 int64
	maxLagTime         time.Duration
	lastCaughtUp       time.Time
	lastSeen           time.Time
	requests           chan *proto.ReplicationRequest
	mu                 sync.RWMutex
	preemptReplication context.CancelFunc
	leader             string
	epoch              uint64
}

func (r *replicator) start(epoch uint64, stop chan struct{}) {
	r.mu.Lock()
	r.epoch = epoch
	now := time.Now()
	r.lastSeen = now
	r.lastCaughtUp = now
	r.mu.Unlock()

	go r.tick(stop)

	var req *proto.ReplicationRequest
	for {
		select {
		case <-stop:
			r.preemptReplication()
			return
		case req = <-r.requests:
		}

		now := time.Now()
		r.mu.Lock()
		r.lastSeen = now

		// Update the ISR replica's latest offset for the stream. This is used
		// by the leader to know when to commit messages.
		r.stream.updateISRLatestOffset(r.replica, req.Offset)

		// Check if we're caught up.
		newest := r.stream.log.NewestOffset()
		if req.Offset >= newest {
			r.lastCaughtUp = now
			r.mu.Unlock()
			r.sendHW(req.Inbox)
			continue
		}

		// Preempt previous replication.
		r.preemptReplication()

		ctx, cancel := context.WithCancel(context.Background())
		r.preemptReplication = cancel
		reader, err := r.stream.log.NewReaderUncommitted(ctx, req.Offset+1)
		if err != nil {
			r.stream.srv.logger.Errorf(
				"Failed to create replication reader for stream %s "+
					"and replica %s (requested offset %d, latest %d): %v",
				r.stream, r.replica, req.Offset+1, newest, err)
			r.mu.Unlock()
			continue
		}
		r.mu.Unlock()

		go r.replicate(reader, req.Inbox, req.Offset)
	}
}

func (r *replicator) request(req *proto.ReplicationRequest) {
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

			if lastSeenElapsed > r.maxLagTime {
				// If the follower has not sent a request in maxLagTime, stop
				// replicating.
				r.mu.Lock()
				r.preemptReplication()
				r.mu.Unlock()
			}
		} else if !outOfSync && !r.stream.inISR(r.replica) {
			// Add replica back into ISR.
			r.stream.srv.logger.Infof("Replica %s for stream %s caught back up with leader, "+
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

func (r *replicator) replicate(reader io.Reader, inbox string, idx int64) {
	var (
		headersBuf = make([]byte, 12)
		buf        = new(bytes.Buffer)
		tempBuf    []byte
		leftover   bool
	)
	for {
		// Write the leader epoch to the buffer.
		if err := binary.Write(buf, proto.Encoding, r.epoch); err != nil {
			r.stream.srv.logger.Errorf("Failed to write leader epoch to buffer while replicating: %v", err)
			return
		}
		// Reserve space for the HW. This will be replaced with the HW at the
		// time of flush.
		if err := binary.Write(buf, proto.Encoding, int64(0)); err != nil {
			r.stream.srv.logger.Errorf("Failed to write HW to buffer while replicating: %v", err)
			return
		}

		if leftover {
			// If we had a leftover message, write it to the buffer now.
			if err := writeMessageToBuffer(buf, headersBuf, tempBuf); err != nil {
				r.stream.srv.logger.Errorf("Failed to write message to buffer while replicating: %v", err)
				return
			}
			leftover = false
		}

		newestOffset := r.stream.log.NewestOffset()
		for idx <= newestOffset && buf.Len() < replicationMaxSize {
			// If we are caught up but have data buffered, flush the batch
			// before we block for more data.
			if idx == newestOffset && buf.Len() > replicationOverhead {
				break
			}

			// Read the message headers.
			if _, err := reader.Read(headersBuf); err != nil {
				if err == io.EOF {
					// EOF indicates replication was preempted.
					return
				}
				r.stream.srv.logger.Errorf("Failed to read message while replicating: %v", err)
				return
			}
			idx = int64(proto.Encoding.Uint64(headersBuf[0:]))
			size := proto.Encoding.Uint32(headersBuf[8:])
			tempBuf = make([]byte, size)

			// Read the message body.
			if _, err := reader.Read(tempBuf); err != nil {
				if err == io.EOF {
					// EOF indicates replication was preempted.
					return
				}
				r.stream.srv.logger.Errorf("Failed to read message while replicating: %v", err)
				return
			}

			// Check if this message will put us over the batch size limit. If
			// it does, flush the batch now. We'll send the leftover message in
			// the next batch.
			if size+uint32(len(headersBuf))+uint32(buf.Len()) > replicationMaxSize {
				leftover = true
				break
			}

			// Write the message to the buffer.
			if err := writeMessageToBuffer(buf, headersBuf, tempBuf); err != nil {
				r.stream.srv.logger.Errorf("Failed to write message to buffer while replicating: %v", err)
				return
			}
		}

		// Set the HW and flush the batch.
		data := buf.Bytes()
		proto.Encoding.PutUint64(data[8:], uint64(r.stream.log.HighWatermark()))
		r.stream.srv.ncRepl.Publish(inbox, data)
		buf.Reset()
	}
}

func writeMessageToBuffer(buf *bytes.Buffer, headers, message []byte) error {
	if _, err := buf.Write(headers); err != nil {
		return err
	}
	if _, err := buf.Write(message); err != nil {
		return err
	}
	return nil
}

func (r *replicator) sendHW(inbox string) {
	buf := make([]byte, replicationOverhead)
	proto.Encoding.PutUint64(buf[:8], r.epoch)
	proto.Encoding.PutUint64(buf[8:], uint64(r.stream.log.HighWatermark()))
	r.stream.srv.ncRepl.Publish(inbox, buf)
}
