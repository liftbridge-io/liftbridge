package server

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	client "github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/liftbridge-io/liftbridge/server/commitlog"
	"github.com/liftbridge-io/liftbridge/server/proto"
)

// recvChannelSize specifies the size of the channel that feeds the leader
// message processing loop.
const recvChannelSize = 64 * 1024

var (
	// envelopeCookie is a magic value that indicates if a NATS message is a
	// structured message protobuf.
	envelopeCookie    = []byte("LIFT")
	envelopeCookieLen = len(envelopeCookie)

	// timestamp returns the current time in Unix nanoseconds. This function
	// exists for mocking purposes.
	timestamp = func() int64 { return time.Now().UnixNano() }
)

// replica tracks the latest log offset for a particular stream replica.
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

// stream represents a replicated message stream backed by a durable commit
// log. A stream is attached to a NATS subject and stores messages on that
// subject in a file-backed log. A stream has a set of replicas assigned to it,
// which are the brokers responsible for replicating the stream. The ISR, or
// in-sync replicas set, is the set of replicas which are currently caught up
// with the stream leader's log. If a replica falls behind, it will be removed
// from the ISR. Followers replicate the leader's log by fetching messages from
// it. All stream access should go through exported methods.
type stream struct {
	*proto.Stream
	mu              sync.RWMutex
	sub             *nats.Subscription // Subscription to stream NATS subject
	leaderReplSub   *nats.Subscription // Subscription for replication requests from followers
	leaderOffsetSub *nats.Subscription // Subscription for leader epoch offset requests from followers
	recvChan        chan *nats.Msg     // Channel leader places received messages on
	log             CommitLog
	srv             *Server
	subjectHash     string
	isLeading       bool
	isFollowing     bool
	replicas        map[string]struct{}
	isr             map[string]*replica
	replicators     map[string]*replicator
	commitQueue     *queue.Queue
	commitCheck     chan struct{}
	recovered       bool
	stopFollower    chan struct{}
	stopLeader      chan struct{}
	belowMinISR     bool
	pause           bool // Pause replication on the leader (for unit testing)
	shutdown        sync.WaitGroup
}

// newStream creates a new stream. If the stream is recovered, it should not be
// started until the recovery process has completed to avoid starting it in an
// intermediate state. This call will initialize or recover the stream's
// backing commit log or return an error if it fails to do so.
func (s *Server) newStream(protoStream *proto.Stream, recovered bool) (*stream, error) {
	var (
		file     = filepath.Join(s.config.DataDir, "streams", protoStream.Subject, protoStream.Name)
		name     = fmt.Sprintf("[subject=%s, name=%s]", protoStream.Subject, protoStream.Name)
		log, err = commitlog.New(commitlog.Options{
			Stream:               name,
			Path:                 file,
			MaxSegmentBytes:      s.config.Log.SegmentMaxBytes,
			MaxLogBytes:          s.config.Log.RetentionMaxBytes,
			MaxLogMessages:       s.config.Log.RetentionMaxMessages,
			MaxLogAge:            s.config.Log.RetentionMaxAge,
			LogRollTime:          s.config.Log.LogRollTime,
			CleanerInterval:      s.config.Log.CleanerInterval,
			Compact:              s.config.Log.Compact,
			CompactMaxGoroutines: s.config.Log.CompactMaxGoroutines,
			Logger:               s.logger,
		})
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create commit log")
	}

	h := sha1.New()
	h.Write([]byte(protoStream.Subject))
	subjectHash := fmt.Sprintf("%x", h.Sum(nil))

	replicas := make(map[string]struct{}, len(protoStream.Replicas))
	for _, replica := range protoStream.Replicas {
		replicas[replica] = struct{}{}
	}

	isr := make(map[string]*replica, len(protoStream.Isr))
	for _, rep := range protoStream.Isr {
		offset := int64(-1)
		// For this server, initialize the replica offset to the newest offset.
		if rep == s.config.Clustering.ServerID {
			offset = log.NewestOffset()
		}
		isr[rep] = &replica{offset: offset}
	}

	st := &stream{
		Stream:      protoStream,
		log:         log,
		srv:         s,
		subjectHash: subjectHash,
		replicas:    replicas,
		isr:         isr,
		commitCheck: make(chan struct{}, len(protoStream.Replicas)),
		recovered:   recovered,
	}

	return st, nil
}

// String returns a human-readable string representation of the stream.
func (s *stream) String() string {
	return fmt.Sprintf("[subject=%s, name=%s]", s.Subject, s.Name)
}

// Close stops the stream if it is running and closes the commit log.
func (s *stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isFollowing {
		if err := s.stopFollowing(); err != nil {
			return err
		}
	} else if s.isLeading {
		if err := s.stopLeading(); err != nil {
			return err
		}
	}

	return s.log.Close()
}

// SetLeader sets the leader for the stream to the given replica and leader
// epoch. If the stream's current leader epoch is greater than the given epoch,
// this returns an error. This will also start the stream as a leader or
// follower, if applicable, unless the stream is in recovery mode.
func (s *stream) SetLeader(leader string, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if epoch < s.LeaderEpoch {
		return fmt.Errorf("proposed leader epoch %d is less than current epoch %d",
			epoch, s.LeaderEpoch)
	}
	s.Leader = leader
	s.LeaderEpoch = epoch

	if s.recovered {
		// If this stream is being recovered, we will start the leader/follower
		// loop later.
		return nil
	}

	return s.startLeadingOrFollowing()
}

// StartRecovered starts the stream as a leader or follower, if applicable, if
// it's in recovery mode. This should be called for each stream after the
// recovery process completes.
func (s *stream) StartRecovered() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.recovered {
		return false, nil
	}
	if err := s.startLeadingOrFollowing(); err != nil {
		return false, err
	}
	s.recovered = false
	return true, nil
}

// startLeadingOrFollowing starts the stream as a leader or follower, if
// applicable.
func (s *stream) startLeadingOrFollowing() error {
	if s.Leader == s.srv.config.Clustering.ServerID {
		s.srv.logger.Debugf("Server becoming leader for stream %s, epoch: %d", s, s.LeaderEpoch)
		if err := s.becomeLeader(s.LeaderEpoch); err != nil {
			s.srv.logger.Errorf("Server failed becoming leader for stream %s: %v", s, err)
			return err
		}
	} else if s.inReplicas(s.srv.config.Clustering.ServerID) {
		s.srv.logger.Debugf("Server becoming follower for stream %s, epoch: %d", s, s.LeaderEpoch)
		if err := s.becomeFollower(); err != nil {
			s.srv.logger.Errorf("Server failed becoming follower for stream %s: %v", s, err)
			return err
		}
	}
	return nil
}

// GetLeader returns the replica that is the stream leader and the leader
// epoch.
func (s *stream) GetLeader() (string, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Leader, s.LeaderEpoch
}

// IsLeader indicates if this server is the stream leader.
func (s *stream) IsLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isLeading
}

// becomeLeader is called when the server has become the leader for this
// stream.
func (s *stream) becomeLeader(epoch uint64) error {
	if s.isFollowing {
		// Stop following if previously a follower.
		if err := s.stopFollowing(); err != nil {
			return err
		}
	} else if s.isLeading {
		// If previously a leader, we need to reset.
		if err := s.stopLeading(); err != nil {
			return err
		}
	}

	if !s.recovered {
		// Update leader epoch on log if this isn't a recovered stream. A
		// recovered stream indicates we were the previous leader and are
		// continuing a leader epoch.
		if err := s.log.NewLeaderEpoch(epoch); err != nil {
			return errors.Wrap(err, "failed to update leader epoch on log")
		}
	}

	// Start message processing loop.
	s.recvChan = make(chan *nats.Msg, recvChannelSize)
	s.stopLeader = make(chan struct{})
	s.srv.startGoroutine(func() {
		s.messageProcessingLoop(s.recvChan, s.stopLeader, epoch)
		s.shutdown.Done()
	})

	// Start replicating to followers.
	s.startReplicating(epoch, s.stopLeader)

	// Subscribe to the NATS subject and begin sequencing messages.
	// TODO: This should be drained on shutdown.
	sub, err := s.srv.nc.QueueSubscribe(s.Subject, s.Group, func(m *nats.Msg) {
		s.recvChan <- m
	})
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to NATS")
	}
	sub.SetPendingLimits(-1, -1)
	s.sub = sub
	s.srv.nc.Flush()

	// Subscribe to the stream replication subject.
	sub, err = s.srv.ncRepl.Subscribe(s.getReplicationRequestInbox(), s.handleReplicationRequest)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to replication inbox")
	}
	sub.SetPendingLimits(-1, -1)
	s.leaderReplSub = sub

	// Also subscribe to leader epoch offset requests subject.
	sub, err = s.srv.ncRepl.Subscribe(s.getLeaderOffsetRequestInbox(), s.handleLeaderOffsetRequest)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to replication inbox")
	}
	sub.SetPendingLimits(-1, -1)
	s.leaderOffsetSub = sub
	s.srv.ncRepl.Flush()

	s.isLeading = true
	s.isFollowing = false

	return nil
}

// stopLeading causes the stream to step down as leader by unsubscribing from
// the NATS subject and replication subject, stopping message processing and
// replication, and disposing the commit queue.
func (s *stream) stopLeading() error {
	// Unsubscribe from NATS subject.
	if err := s.sub.Unsubscribe(); err != nil {
		return err
	}

	// Unsubscribe from replication subject.
	if err := s.leaderReplSub.Unsubscribe(); err != nil {
		return err
	}

	// Stop processing messages and replicating.
	s.shutdown.Add(1) // Message processing loop
	s.shutdown.Add(1) // Commit loop
	if replicas := len(s.replicas); replicas > 1 {
		s.shutdown.Add(replicas - 1) // Replicator loops (minus one to exclude self)
	}
	close(s.stopLeader)

	// Wait for loops to shutdown.
	s.shutdown.Wait()

	s.commitQueue.Dispose()
	s.isLeading = false

	return nil
}

// becomeFollower is called when the server has become a follower for this
// stream.
func (s *stream) becomeFollower() error {
	if s.isLeading {
		// Stop leading if previously a leader.
		if err := s.stopLeading(); err != nil {
			return err
		}
	} else if s.isFollowing {
		// If previously a follower, we need to reset.
		if err := s.stopFollowing(); err != nil {
			return err
		}
	}

	// Truncate potentially uncommitted messages from the log.
	if err := s.truncateUncommitted(); err != nil {
		return errors.Wrap(err, "failed to truncate log")
	}

	// Start fetching messages from the leader's log starting at the HW.
	s.stopFollower = make(chan struct{})
	s.srv.logger.Debugf("Replicating stream %s from leader %s", s, s.Leader)
	s.srv.startGoroutine(func() {
		s.replicationRequestLoop(s.Leader, s.LeaderEpoch, s.stopFollower)
	})

	s.isFollowing = true
	s.isLeading = false

	return nil
}

// stopFollowing causes the stream to step down as a follower by stopping
// replication requests and the leader failure detector.
func (s *stream) stopFollowing() error {
	// Stop replication request and leader failure detector loop.
	// TODO: Do graceful shutdown similar to stopLeading().
	close(s.stopFollower)
	s.isFollowing = false
	return nil
}

// handleLeaderOffsetRequest is a NATS handler that's invoked when the leader
// receives a leader epoch offset request from a follower. The request will
// contain the latest leader epoch in the follower's leader epoch sequence.
// This will send the last offset for the requested leader epoch, i.e. the
// start offset of the first leader epoch larger than the requested leader
// epoch or the log end offset if the leader's current epoch is equal to the
// one requested.
func (s *stream) handleLeaderOffsetRequest(msg *nats.Msg) {
	req := &proto.LeaderEpochOffsetRequest{}
	if err := req.Unmarshal(msg.Data); err != nil {
		s.srv.logger.Errorf("Invalid leader epoch offset request for stream %s: %v", s, err)
		return
	}
	resp, err := (&proto.LeaderEpochOffsetResponse{
		EndOffset: s.log.LastOffsetForLeaderEpoch(req.LeaderEpoch),
	}).Marshal()
	if err != nil {
		panic(err)
	}
	if err := msg.Respond(resp); err != nil {
		s.srv.logger.Errorf("Failed to respond to leader offset request: %v", err)
	}
}

// handleReplicationRequest is a NATS handler that's invoked when the leader
// receives a replication request from a follower. It will send messages to the
// NATS subject specified on the request.
func (s *stream) handleReplicationRequest(msg *nats.Msg) {
	req := &proto.ReplicationRequest{}
	if err := req.Unmarshal(msg.Data); err != nil {
		s.srv.logger.Errorf("Invalid replication request for stream %s: %v", s, err)
		return
	}
	s.mu.Lock()
	if s.pause {
		s.mu.Unlock()
		return
	}
	if _, ok := s.replicas[req.ReplicaID]; !ok {
		s.srv.logger.Warnf("Received replication request for stream %s from non-replica %s",
			s, req.ReplicaID)
		s.mu.Unlock()
		return
	}
	replicator, ok := s.replicators[req.ReplicaID]
	if !ok {
		panic(fmt.Sprintf("No replicator for stream %s and replica %s", s, req.ReplicaID))
	}
	s.mu.Unlock()
	replicator.request(replicationRequest{req, msg})
}

// handleReplicationResponse is a NATS handler that's invoked when a follower
// receives a replication response from the leader. This response will contain
// the leader epoch, leader HW, and (optionally) messages to replicate.
func (s *stream) handleReplicationResponse(msg *nats.Msg) int {
	// We should have at least 16 bytes, 8 for leader epoch and 8 for HW.
	if len(msg.Data) < 16 {
		s.srv.logger.Warnf("Invalid replication response for stream %s", s)
		return 0
	}

	leaderEpoch := proto.Encoding.Uint64(msg.Data[:8])

	s.mu.RLock()
	if !s.isFollowing {
		s.mu.RUnlock()
		return 0
	}

	if s.LeaderEpoch != leaderEpoch {
		s.mu.RUnlock()
		return 0
	}
	s.mu.RUnlock()

	// Update HW from leader's HW.
	hw := int64(proto.Encoding.Uint64(msg.Data[8:]))
	s.log.SetHighWatermark(hw)

	data := msg.Data[16:]
	if len(data) == 0 {
		return 0
	}

	// We should have at least 28 bytes for headers.
	if len(data) <= 28 {
		s.srv.logger.Warnf("Invalid replication response for stream %s", s)
		return 0
	}
	offset := int64(proto.Encoding.Uint64(data[:8]))
	if offset < s.log.NewestOffset()+1 {
		return 0
	}
	offsets, err := s.log.AppendMessageSet(data)
	if err != nil {
		panic(fmt.Errorf("Failed to replicate data to log %s: %v", s, err))
	}
	return len(offsets)
}

// getReplicationRequestInbox returns the NATS subject to send replication
// requests to.
func (s *stream) getReplicationRequestInbox() string {
	return fmt.Sprintf("%s.%s.%s.replicate",
		s.srv.config.Clustering.Namespace, s.subjectHash, s.Name)
}

// getLeaderOffsetRequestInbox returns the NATS subject to send leader epoch
// offset requests to.
func (s *stream) getLeaderOffsetRequestInbox() string {
	return fmt.Sprintf("%s.%s.%s.offset",
		s.srv.config.Clustering.Namespace, s.subjectHash, s.Name)
}

// messageProcessingLoop is a long-running loop that processes messages
// received on the given channel until the stop channel is closed. This will
// attempt to batch messages up before writing them to the commit log. Once
// written to the write-ahead log, a marker is written to the commit queue to
// indicate it's pending commit. Once the ISR has replicated the message, the
// leader commits it by removing it from the queue and sending an
// acknowledgement to the client.
func (s *stream) messageProcessingLoop(recvChan <-chan *nats.Msg, stop <-chan struct{},
	leaderEpoch uint64) {

	var (
		msg       *nats.Msg
		batchSize = s.srv.config.BatchMaxMessages
		batchWait = s.srv.config.BatchWaitTime
		msgBatch  = make([]*proto.Message, 0, batchSize)
	)
	for {
		msgBatch = msgBatch[:0]
		select {
		case <-stop:
			return
		case msg = <-recvChan:
		}

		m := natsToProtoMessage(msg, leaderEpoch)
		msgBatch = append(msgBatch, m)
		remaining := batchSize - 1

		// Fill the batch up to the max batch size or until the channel is
		// empty.
		for remaining > 0 {
			chanLen := len(recvChan)
			if chanLen == 0 {
				if batchWait > 0 {
					time.Sleep(batchWait)
					chanLen = len(recvChan)
					if chanLen == 0 {
						break
					}
				} else {
					break
				}
			}

			if chanLen > remaining {
				chanLen = remaining
			}

			for i := 0; i < chanLen; i++ {
				msg = <-recvChan
				m := natsToProtoMessage(msg, leaderEpoch)
				msgBatch = append(msgBatch, m)
			}
			remaining -= chanLen
		}

		// Write uncommitted messages to log.
		offsets, err := s.log.Append(msgBatch)
		if err != nil {
			s.srv.logger.Errorf("Failed to append to log %s: %v", s, err)
			return
		}

		for i, msg := range msgBatch {
			s.processPendingMessage(offsets[i], msg)
		}

		// Update this replica's latest offset.
		s.updateISRLatestOffset(
			s.srv.config.Clustering.ServerID,
			offsets[len(offsets)-1],
		)
	}
}

// processPendingMessage sends an ack if the message's AckPolicy is LEADER and
// adds the pending message to the commit queue. Messages are removed from the
// queue and committed when the entire ISR has replicated them.
func (s *stream) processPendingMessage(offset int64, msg *proto.Message) {
	ack := &client.Ack{
		StreamSubject: s.Subject,
		StreamName:    s.Name,
		MsgSubject:    string(msg.Headers["subject"]),
		Offset:        offset,
		AckInbox:      msg.AckInbox,
		CorrelationId: msg.CorrelationID,
		AckPolicy:     msg.AckPolicy,
	}
	if msg.AckPolicy == client.AckPolicy_LEADER {
		// Send the ack now since AckPolicy_LEADER means we ack as soon as the
		// leader has written the message to its WAL.
		s.sendAck(ack)
	}
	if err := s.commitQueue.Put(ack); err != nil {
		// This is very bad and should not happen.
		panic(fmt.Sprintf("Failed to add message to commit queue: %v", err))
	}
}

// startReplicating starts a long-running goroutine which handles committing
// messages in the commit queue and a replication goroutine for each replica.
func (s *stream) startReplicating(epoch uint64, stop chan struct{}) {
	if s.ReplicationFactor > 1 {
		s.srv.logger.Debugf("Replicating stream %s to followers", s)
	}
	s.commitQueue = queue.New(100)
	s.srv.startGoroutine(func() {
		s.commitLoop(stop)
		s.shutdown.Done()
	})

	s.replicators = make(map[string]*replicator, len(s.replicas)-1)
	for replica := range s.replicas {
		if replica == s.srv.config.Clustering.ServerID {
			// Don't replicate to ourselves.
			continue
		}
		r := &replicator{
			replica:    replica,
			stream:     s,
			requests:   make(chan replicationRequest, 1),
			maxLagTime: s.srv.config.Clustering.ReplicaMaxLagTime,
			leader:     s.srv.config.Clustering.ServerID,
		}
		s.replicators[replica] = r
		s.srv.startGoroutine(func() {
			r.start(epoch, stop)
			s.shutdown.Done()
		})
	}
}

// commitLoop is a long-running loop which checks to see if messages in the
// commit queue can be committed and, if so, removes them from the queue and
// sends client acks. It runs until the stop channel is closed.
func (s *stream) commitLoop(stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-s.commitCheck:
		}

		s.mu.RLock()

		// Check if the ISR size is below the minimum ISR size. If it is, we
		// cannot commit any messages.
		var (
			minISR  = s.srv.config.Clustering.MinISR
			isrSize = len(s.isr)
		)
		if isrSize < minISR {
			s.mu.RUnlock()
			s.srv.logger.Errorf(
				"Unable to commit messages for stream %s, ISR size (%d) below minimum (%d)",
				s, isrSize, minISR)
			continue
		}

		// Commit all messages in the queue that have been replicated by all
		// replicas in the ISR. Do this by taking the min of all latest offsets
		// in the ISR, updating the HW, and acking queue entries.
		var (
			latestOffsets = make([]int64, isrSize)
			i             = 0
		)
		for _, replica := range s.isr {
			latestOffsets[i] = replica.getLatestOffset()
			i++
		}
		s.mu.RUnlock()
		var (
			minLatest      = min(latestOffsets)
			committed, err = s.commitQueue.TakeUntil(func(pending interface{}) bool {
				return pending.(*client.Ack).Offset <= minLatest
			})
		)

		s.log.SetHighWatermark(minLatest)

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
				s.sendAck(ack)
			}
		}
	}
}

// sendAck publishes an ack to the specified AckInbox. If no AckInbox is set,
// this does nothing.
func (s *stream) sendAck(ack *client.Ack) {
	if ack.AckInbox == "" {
		return
	}
	data, err := ack.Marshal()
	if err != nil {
		panic(err)
	}
	s.srv.ncAcks.Publish(ack.AckInbox, data)
}

// replicationRequestLoop is a long-running loop which sends replication
// requests to the stream leader, handles replicating messages, and checks the
// health of the leader.
func (s *stream) replicationRequestLoop(leader string, epoch uint64, stop chan struct{}) {
	var (
		emptyResponseCount int
		leaderLastSeen     = time.Now()
	)
	for {
		select {
		case <-stop:
			return
		default:
		}

		replicated, err := s.sendReplicationRequest()
		if err != nil {
			s.srv.logger.Errorf(
				"Error sending replication request for stream %s: %v", s, err)
		} else {
			leaderLastSeen = time.Now()
		}

		select {
		case <-stop:
			return
		default:
		}

		// Check if leader has exceeded max leader timeout.
		s.checkLeaderHealth(leader, epoch, leaderLastSeen)

		// TODO: Make this smarter. Probably have the request wait for data on
		// the server side. Need to handle timeouts better too.
		if replicated == 0 {
			time.Sleep(computeReplicaFetchSleep(emptyResponseCount))
			emptyResponseCount++
		} else {
			emptyResponseCount = 0
		}
	}
}

// checkLeaderHealth checks if the leader has responded within
// ReplicaMaxLeaderTimeout and, if not, reports the leader to the controller.
func (s *stream) checkLeaderHealth(leader string, epoch uint64, leaderLastSeen time.Time) {
	lastSeenElapsed := time.Since(leaderLastSeen)
	if lastSeenElapsed > s.srv.config.Clustering.ReplicaMaxLeaderTimeout {
		// Leader has not sent a response in ReplicaMaxLeaderTimeout, so report
		// it to controller.
		s.srv.logger.Errorf("Leader %s for stream %s exceeded max leader timeout "+
			"(last seen: %s), reporting leader to controller",
			leader, s, lastSeenElapsed)
		req := &proto.ReportLeaderOp{
			Subject:     s.Subject,
			Name:        s.Name,
			Replica:     s.srv.config.Clustering.ServerID,
			Leader:      leader,
			LeaderEpoch: epoch,
		}
		if err := s.srv.metadata.ReportLeader(context.Background(), req); err != nil {
			s.srv.logger.Errorf("Failed to report leader %s for stream %s: %s",
				leader, s, err.Err())
		}
	}
}

// computeReplicaFetchSleep calculates the time to backoff before sending
// another replication request.
func computeReplicaFetchSleep(emptyResponseCount int) time.Duration {
	sleep := time.Duration(300+emptyResponseCount*5) * time.Millisecond
	if sleep > time.Second {
		sleep = time.Second
	}
	return sleep
}

// sendReplicationRequest sends a replication request to the stream leader and
// processes the response.
func (s *stream) sendReplicationRequest() (int, error) {
	data, err := (&proto.ReplicationRequest{
		ReplicaID: s.srv.config.Clustering.ServerID,
		Offset:    s.log.NewestOffset(),
	}).Marshal()
	if err != nil {
		panic(err)
	}
	resp, err := s.srv.ncRepl.Request(
		s.getReplicationRequestInbox(),
		data,
		s.srv.config.Clustering.ReplicaFetchTimeout,
	)
	if err != nil {
		return 0, err
	}
	replicated := s.handleReplicationResponse(resp)
	return replicated, nil
}

// truncateUncommitted truncates the log up to the start offset of the first
// leader epoch larger than the current epoch. This removes any potentially
// uncommitted messages in the log.
func (s *stream) truncateUncommitted() error {
	// Request the last offset for the epoch from the leader.
	var (
		lastOffset  int64
		err         error
		leaderEpoch = s.log.LastLeaderEpoch()
	)
	for i := 0; i < 3; i++ {
		lastOffset, err = s.sendLeaderOffsetRequest(leaderEpoch)
		// Retry timeouts.
		if err == nats.ErrTimeout {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	if err != nil {
		s.srv.logger.Errorf(
			"Failed to fetch last offset for leader epoch for stream %s: %v",
			s, err)
		// Fall back to HW truncation if we fail to fetch last offset for
		// leader epoch.
		// TODO: Should this be configurable since there is potential for data
		// loss or replica divergence?
		return s.truncateToHW()
	}

	s.srv.logger.Debugf("Truncating log for stream %s to %d", s, lastOffset)
	// Add 1 because we don't want to truncate the last offset itself.
	return s.log.Truncate(lastOffset + 1)
}

// sendLeaderOffsetRequest sends a request to the leader for the last offset
// for the current leader epoch.
func (s *stream) sendLeaderOffsetRequest(leaderEpoch uint64) (int64, error) {
	data, err := (&proto.LeaderEpochOffsetRequest{LeaderEpoch: leaderEpoch}).Marshal()
	if err != nil {
		panic(err)
	}
	resp, err := s.srv.ncRepl.Request(
		s.getLeaderOffsetRequestInbox(),
		data,
		time.Second,
	)
	if err != nil {
		return 0, err
	}
	offsetResp := &proto.LeaderEpochOffsetResponse{}
	if err := offsetResp.Unmarshal(resp.Data); err != nil {
		return 0, err
	}
	return offsetResp.EndOffset, nil
}

// truncateToHW truncates the log up to the latest high watermark. This removes
// any potentially uncommitted messages in the log. However, this should only
// be used as a fallback in the event that epoch-based truncation fails. There
// are a couple edge cases with this method of truncating the log that could
// result in data loss or replica divergence (see issue #38).
func (s *stream) truncateToHW() error {
	var (
		newestOffset = s.log.NewestOffset()
		hw           = s.log.HighWatermark()
	)
	if newestOffset == hw {
		return nil
	}
	s.srv.logger.Debugf("Truncating log for stream %s to HW %d", s, hw)
	// Add 1 because we don't want to truncate the HW itself.
	return s.log.Truncate(hw + 1)
}

// inISR indicates if the given replica is in the current in-sync replicas set.
func (s *stream) inISR(replica string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.isr[replica]
	return ok
}

// inReplicas indicates if the given broker is a replica for the stream.
func (s *stream) inReplicas(id string) bool {
	_, ok := s.replicas[id]
	return ok
}

// RemoveFromISR removes the given replica from the in-sync replicas set. It
// returns an error if the broker is not a stream replica. This will also
// insert a check to see if pending messages need to be committed since the ISR
// shrank.
func (s *stream) RemoveFromISR(replica string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inReplicas(replica) {
		return fmt.Errorf("%s not a replica", replica)
	}
	delete(s.isr, replica)

	// Check if ISR went below minimum ISR size. This is important for
	// operators to be aware of.
	var (
		minISR  = s.srv.config.Clustering.MinISR
		isrSize = len(s.isr)
	)
	if !s.belowMinISR && isrSize < minISR {
		s.srv.logger.Errorf("ISR for stream %s has shrunk below minimum size %d, currently %d",
			s, minISR, isrSize)
		s.belowMinISR = true
	}

	// We may need to commit messages since the ISR shrank.
	if s.isLeading {
		select {
		case s.commitCheck <- struct{}{}:
		default:
		}
	}

	return nil
}

// AddToISR adds the given replica to the in-sync replicas set. It returns an
// error if the broker is not a stream replica.
func (s *stream) AddToISR(rep string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inReplicas(rep) {
		return fmt.Errorf("%s not a replica", rep)
	}
	s.isr[rep] = &replica{offset: -1}

	// Check if ISR recovered from being below the minimum ISR size.
	var (
		minISR  = s.srv.config.Clustering.MinISR
		isrSize = len(s.isr)
	)
	if s.belowMinISR && isrSize >= minISR {
		s.srv.logger.Infof("ISR for stream %s has recovered from being below minimum size %d, currently %d",
			s, minISR, isrSize)
		s.belowMinISR = false
	}

	return nil
}

// GetEpoch returns the current stream epoch. The epoch is a monotonically
// increasing number which increases when a change is made to the stream. This
// is used to determine if an operation is outdated.
func (s *stream) GetEpoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Epoch
}

// SetEpoch sets the current stream epoch. See GetEpoch for information on the
// epoch's purpose.
func (s *stream) SetEpoch(epoch uint64) {
	s.mu.Lock()
	s.Epoch = epoch
	s.mu.Unlock()
}

// Marshal serializes the stream into a byte slice.
func (s *stream) Marshal() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := s.Stream.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

// ISRSize returns the current number of replicas in the in-sync replicas set.
func (s *stream) ISRSize() int {
	s.mu.RLock()
	size := len(s.isr)
	s.mu.RUnlock()
	return size
}

// GetISR returns the in-sync replicas set.
func (s *stream) GetISR() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	isr := make([]string, 0, len(s.isr))
	for replica := range s.isr {
		isr = append(isr, replica)
	}
	return isr
}

// GetReplicas returns the list of all brokers which are replicas for the
// stream.
func (s *stream) GetReplicas() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	replicas := make([]string, 0, len(s.replicas))
	for replica := range s.replicas {
		replicas = append(replicas, replica)
	}
	return replicas
}

// updateISRLatestOffset updates the given replica's latest log offset. When a
// replica's latest log offset increases, we check to see if anything in the
// commit queue can be committed.
func (s *stream) updateISRLatestOffset(replica string, offset int64) {
	s.mu.RLock()
	rep, ok := s.isr[replica]
	s.mu.RUnlock()
	if !ok {
		// Replica is not currently in ISR.
		return
	}
	if rep.updateLatestOffset(offset) {
		// If offset updated, we may need to commit messages.
		select {
		case s.commitCheck <- struct{}{}:
		default:
		}
	}
}

// pauseReplication stops replication on the leader. This is for unit testing
// purposes only.
func (s *stream) pauseReplication() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pause = true
}

// getMessage converts the given payload into a client Message if it is one.
// This is indicated by the presence of the envelope cookie. If it is not, nil
// is returned.
func getMessage(data []byte) *client.Message {
	if len(data) <= 4 {
		return nil
	}
	if !bytes.Equal(data[0:4], envelopeCookie) {
		return nil
	}
	msg := &client.Message{}
	if err := msg.Unmarshal(data[4:]); err != nil {
		return nil
	}
	return msg
}

// natsToProtoMessage converts the given NATS message to a proto Message.
func natsToProtoMessage(msg *nats.Msg, leaderEpoch uint64) *proto.Message {
	message := getMessage(msg.Data)
	m := &proto.Message{
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
	} else {
		m.Value = msg.Data
	}
	m.Headers["subject"] = []byte(msg.Subject)
	m.Headers["reply"] = []byte(msg.Reply)
	return m
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
