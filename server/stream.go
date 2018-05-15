package server

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	client "github.com/tylertreat/go-jetbridge/proto"
	"golang.org/x/net/context"

	"github.com/tylertreat/jetbridge/server/commitlog"
	"github.com/tylertreat/jetbridge/server/proto"
)

var envelopeCookie = []byte("jetb")

type replica struct {
	mu     sync.RWMutex
	offset int64
}

func (r *replica) updateLatestOffset(offset int64) (updated bool) {
	r.mu.Lock()
	if offset > r.offset {
		r.offset = offset
		updated = true
	}
	r.mu.Unlock()
	return
}

type stream struct {
	*proto.Stream
	mu              sync.RWMutex
	sub             *nats.Subscription // Subscription to stream NATS subject
	leaderReplSub   *nats.Subscription // Subscription for replication requests from followers
	followerReplSub *nats.Subscription // Subscription for replication responses from leader
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
	leaderLastSeen  time.Time
	leaderEpoch     uint64
}

func (s *Server) newStream(protoStream *proto.Stream) (*stream, error) {
	log, err := commitlog.New(commitlog.Options{
		Path:            filepath.Join(s.config.DataPath, "streams", protoStream.Subject, protoStream.Name),
		MaxSegmentBytes: s.config.Log.SegmentMaxBytes,
		MaxLogBytes:     s.config.Log.RetentionMaxBytes,
		Compact:         s.config.Log.Compact,
		Logger:          s.logger,
	})
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
		isr[rep] = &replica{offset: -1}
	}

	st := &stream{
		Stream:      protoStream,
		log:         log,
		srv:         s,
		subjectHash: subjectHash,
		replicas:    replicas,
		isr:         isr,
		replicators: make(map[string]*replicator, len(protoStream.Replicas)),
		commitCheck: make(chan struct{}, 1),
	}

	for _, replica := range protoStream.Replicas {
		if replica == s.config.Clustering.ServerID {
			// Don't replicate to ourselves.
			continue
		}
		st.replicators[replica] = &replicator{
			replica:           replica,
			stream:            st,
			requests:          make(chan *proto.ReplicationRequest),
			hw:                -1,
			maxLagTime:        s.config.Clustering.ReplicaMaxLagTime,
			leader:            s.config.Clustering.ServerID,
			cancelReplication: func() {}, // Intentional no-op initially
		}
	}

	return st, nil
}

func (s *stream) String() string {
	return fmt.Sprintf("[subject=%s, name=%s]", s.Subject, s.Name)
}

func (s *stream) close() error {
	if err := s.log.Close(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.sub != nil {
		if err := s.sub.Unsubscribe(); err != nil {
			return err
		}
	}
	if s.leaderReplSub != nil {
		if err := s.leaderReplSub.Unsubscribe(); err != nil {
			return err
		}
	}
	if s.followerReplSub != nil {
		return s.followerReplSub.Unsubscribe()
	}
	return nil
}

func (s *stream) setLeader(leader string, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch < s.LeaderEpoch {
		return fmt.Errorf("proposed leader epoch %d is less than current epoch %d",
			epoch, s.LeaderEpoch)
	}
	s.Leader = leader
	s.LeaderEpoch = epoch

	if leader == s.srv.config.Clustering.ServerID {
		if err := s.becomeLeader(epoch); err != nil {
			return err
		}
	} else if s.inReplicas(s.srv.config.Clustering.ServerID) {
		if err := s.becomeFollower(); err != nil {
			return err
		}
	}

	return nil
}

func (s *stream) getLeader() (string, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Leader, s.LeaderEpoch
}

// becomeLeader is called when the server has become the leader for this
// stream.
func (s *stream) becomeLeader(epoch uint64) error {
	if s.isLeading {
		return nil
	}

	// If previously follower, unsubscribe from replication subject.
	if s.isFollowing {
		if err := s.followerReplSub.Unsubscribe(); err != nil {
			return err
		}
	}

	// Start replicating to followers.
	s.startReplicating(epoch)

	// Subscribe to the NATS subject and begin sequencing messages.
	sub, err := s.srv.nc.QueueSubscribe(s.Subject, s.ConsumerGroup, s.handleMsg)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to NATS")
	}
	s.sub = sub

	// Also subscribe to the stream replication subject.
	sub, err = s.srv.ncRepl.Subscribe(s.getReplicationRequestInbox(), s.handleReplicationRequest)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to replication inbox")
	}
	s.leaderReplSub = sub

	s.srv.logger.Debugf("Server became leader for stream %s", s)
	s.isLeading = true
	s.isFollowing = false

	return nil
}

// becomeFollower is called when the server has become a follower for this
// stream.
func (s *stream) becomeFollower() error {
	if s.isFollowing {
		return nil
	}

	// If previously leader...
	if s.isLeading {
		// ...unsubscribe from NATS subject.
		if err := s.sub.Unsubscribe(); err != nil {
			return err
		}

		// ...unsubscribe from replication subject.
		if err := s.leaderReplSub.Unsubscribe(); err != nil {
			return err
		}

		// ...and stop replicating.
		s.stopReplicating()
	}

	// Truncate the log up to the latest HW. This removes any potentially
	// uncommitted messages in the log.
	if err := s.truncateToHW(); err != nil {
		return errors.Wrap(err, "failed to truncate log")
	}

	// Subscribe to the stream replication subject to receive messages.
	sub, err := s.srv.ncRepl.Subscribe(s.getReplicationResponseInbox(), s.handleReplicationResponse)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to replication inbox")
	}
	s.followerReplSub = sub

	// Start fetching messages from the leader's log starting at the HW.
	go s.startReplicationRequests()
	go s.startLeaderFailureDetectorLoop()

	s.srv.logger.Debugf("Server became follower for stream %s", s)
	s.isFollowing = true
	s.isLeading = false

	return nil
}

// TODO: I think there is potential for a race where this broker loses
// leadership for the stream while it's receiving messages here. Look into
// this.
func (s *stream) handleMsg(msg *nats.Msg) {
	envelope := getEnvelope(msg.Data)
	m := &proto.Message{
		MagicByte: 2,
		Timestamp: time.Now(),
		Headers:   make(map[string][]byte),
	}
	if envelope != nil {
		m.Key = envelope.Key
		m.Value = envelope.Value
		for key, value := range envelope.Headers {
			m.Headers[key] = value
		}
	} else {
		m.Value = msg.Data
	}
	m.Headers["subject"] = []byte(msg.Subject)
	m.Headers["reply"] = []byte(msg.Reply)

	ms := &proto.MessageSet{Messages: []*proto.Message{m}}
	data, err := proto.Encode(ms)
	if err != nil {
		panic(err)
	}

	// Write uncommitted message to log.
	offset, err := s.log.Append(data)
	if err != nil {
		s.srv.logger.Errorf("Failed to append to log %s: %v", s, err)
		return
	}

	// Update this replica's latest offset.
	s.updateISRLatestOffset(s.srv.config.Clustering.ServerID, offset)

	// If there is an AckInbox, add the pending message to the commit queue. If
	// there isn't one, we don't care whether the message is committed or not.
	if envelope != nil && envelope.AckInbox != "" {
		s.commitQueue.Put(&client.Ack{
			StreamSubject: s.Subject,
			StreamName:    s.Name,
			MsgSubject:    msg.Subject,
			Offset:        offset,
			AckInbox:      envelope.AckInbox,
		})
	}
}

func (s *stream) handleReplicationRequest(msg *nats.Msg) {
	req := &proto.ReplicationRequest{}
	if err := req.Unmarshal(msg.Data); err != nil {
		s.srv.logger.Errorf("Invalid replication request for stream %s: %v", s, err)
		return
	}
	s.mu.Lock()
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
	replicator.request(req)
}

func (s *stream) handleReplicationResponse(msg *nats.Msg) {
	// TODO: check leader epoch.

	s.mu.Lock()
	s.leaderLastSeen = time.Now()
	s.mu.Unlock()

	// We should have at least 8 bytes for HW.
	if len(msg.Data) < 8 {
		s.srv.logger.Warnf("Invalid replication response for stream %s", s)
		return
	}

	// Update HW from leader's HW.
	hw := int64(proto.Encoding.Uint64(msg.Data[:8]))
	s.log.SetHighWatermark(hw)

	data := msg.Data[8:]
	if len(data) == 0 {
		return
	}

	// We should have at least 12 bytes for headers.
	if len(data) <= 12 {
		s.srv.logger.Warnf("Invalid replication response for stream %s", s)
		return
	}
	offset := int64(proto.Encoding.Uint64(data[:8]))
	if offset > s.log.NewestOffset() {
		if _, err := s.log.Append(data); err != nil {
			panic(fmt.Errorf("Failed to replicate data to log %s: %v", s, err))
		}
	}
}

func (s *stream) getReplicationRequestInbox() string {
	return fmt.Sprintf("%s.%s.%s.replicate",
		s.srv.config.Clustering.Namespace, s.subjectHash, s.Name)
}

func (s *stream) getReplicationResponseInbox() string {
	return fmt.Sprintf("%s.%s.%s.replicate.%s",
		s.srv.config.Clustering.Namespace, s.subjectHash, s.Name, s.srv.config.Clustering.ServerID)
}

func (s *stream) startReplicating(epoch uint64) {
	s.commitQueue = queue.New(100) // TODO: hint should be ~steady state inflight messages
	if s.ReplicationFactor > 1 {
		s.srv.logger.Debugf("Replicating stream %s to followers", s)
	}
	go s.startCommitLoop()
	for _, replicator := range s.replicators {
		go replicator.start(epoch)
	}
}

func (s *stream) stopReplicating() {
	for _, replicator := range s.replicators {
		replicator.stop()
	}
	s.commitQueue.Dispose()
}

func (s *stream) startCommitLoop() {
	for {
		<-s.commitCheck
		s.mu.RLock()
		isLeading := s.isLeading
		s.mu.RUnlock()
		if !isLeading {
			return
		}

		// Commit all messages in the queue that have been replicated by all
		// replicas in the ISR. Do this by taking the min of all latest offsets
		// in the ISR.
		s.mu.RLock()
		var (
			latestOffsets = make([]int64, len(s.isr))
			i             = 0
		)
		for _, replica := range s.isr {
			replica.mu.RLock()
			latestOffsets[i] = replica.offset
			replica.mu.RUnlock()
			i++
		}
		s.mu.RUnlock()
		var (
			minLatest     = min(latestOffsets)
			toCommit, err = s.commitQueue.TakeUntil(func(pending interface{}) bool {
				return pending.(*client.Ack).Offset <= minLatest
			})
		)

		// An error here indicates the queue was disposed as a result of the
		// leader stepping down.
		if err != nil {
			return
		}

		if len(toCommit) == 0 {
			continue
		}

		// Commit by updating the HW and sending acks.
		hw := toCommit[len(toCommit)-1].(*client.Ack).Offset
		s.log.SetHighWatermark(hw)
		for _, ackIface := range toCommit {
			ack := ackIface.(*client.Ack)
			data, err := ack.Marshal()
			if err != nil {
				panic(err)
			}
			s.srv.nc.Publish(ack.AckInbox, data)
		}
	}
}

func (s *stream) startReplicationRequests() {
	s.srv.logger.Debugf("Replicating stream %s from leader %s", s, s.Leader)
	ticker := time.NewTicker(s.srv.config.Clustering.ReplicaFetchInterval)
	defer ticker.Stop()
	for {
		<-ticker.C
		s.mu.RLock()
		isFollowing := s.isFollowing
		s.mu.RUnlock()
		if !isFollowing {
			return
		}
		s.sendReplicationRequest()
	}
}

func (s *stream) startLeaderFailureDetectorLoop() {
	s.mu.Lock()
	s.leaderLastSeen = time.Now()
	s.mu.Unlock()
	interval := s.srv.config.Clustering.ReplicaMaxLeaderTimeout / 2
	if interval == 0 {
		interval = s.srv.config.Clustering.ReplicaMaxLeaderTimeout
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		now := <-ticker.C
		s.mu.RLock()
		var (
			leader          = s.Leader
			isFollowing     = s.isFollowing
			lastSeenElapsed = now.Sub(s.leaderLastSeen)
		)
		s.mu.RUnlock()
		if !isFollowing {
			return
		}

		if lastSeenElapsed > s.srv.config.Clustering.ReplicaMaxLeaderTimeout {
			// Leader has not sent a response in ReplicaMaxLeaderTimeout, so
			// report it to controller.
			s.srv.logger.Errorf("Leader %s for stream %s exceeded max leader timeout "+
				"(last seen: %s), reporting leader to controller",
				leader, s, lastSeenElapsed)
			req := &proto.ReportLeaderOp{
				Subject: s.Subject,
				Name:    s.Name,
				Replica: s.srv.config.Clustering.ServerID,
			}
			if err := s.srv.metadata.ReportLeader(context.Background(), req); err != nil {
				s.srv.logger.Errorf("Failed to report leader %s for stream %s: %s",
					leader, s, err.Err())
			} else {
				s.srv.logger.Debugf("Reported leader %s for stream %s", leader, s)
			}
		}
	}
}

func (s *stream) sendReplicationRequest() {
	data, err := (&proto.ReplicationRequest{
		ReplicaID: s.srv.config.Clustering.ServerID,
		Offset:    s.log.NewestOffset(),
		Inbox:     s.getReplicationResponseInbox(),
	}).Marshal()
	if err != nil {
		panic(err)
	}
	s.srv.ncRepl.Publish(s.getReplicationRequestInbox(), data)
}

// truncateToHW truncates the log up to the latest high watermark. This removes
// any potentially uncommitted messages in the log.
//
// TODO: There are a couple edge cases with this method of truncating the log
// that could result in data loss or replica divergence. These can be solved by
// using a leader epoch rather than the high watermark for truncation. This
// solution was implemented in Kafka and is described here:
// https://cwiki.apache.org/confluence/x/oQQIB
func (s *stream) truncateToHW() error {
	var (
		newestOffset = s.log.NewestOffset()
		hw           = s.log.HighWatermark()
	)
	if newestOffset == hw {
		return nil
	}
	s.srv.logger.Debugf("Truncating log for stream %s up to HW %d", s, hw)
	// Add 1 because we don't want to truncate the HW itself.
	return s.log.Truncate(hw + 1)
}

func (s *stream) inISR(replica string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.isr[replica]
	return ok
}

func (s *stream) InReplicas(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inReplicas(id)
}

func (s *stream) inReplicas(id string) bool {
	_, ok := s.replicas[id]
	return ok
}

func (s *stream) removeFromISR(replica string) {
	s.mu.Lock()
	delete(s.isr, replica)
	s.mu.Unlock()
}

func (s *stream) addToISR(rep string) {
	s.mu.Lock()
	s.isr[rep] = &replica{offset: -1}
	s.mu.Unlock()
}

func (s *stream) isrSize() int {
	s.mu.RLock()
	size := len(s.isr)
	s.mu.RUnlock()
	return size
}

func (s *stream) getISR() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	isr := make([]string, 0, len(s.isr))
	for replica, _ := range s.isr {
		isr = append(isr, replica)
	}
	return isr
}

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

func getEnvelope(data []byte) *client.Message {
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

func consumeStreamMessageSet(reader io.Reader, headersBuf []byte) ([]byte, int64, error) {
	if _, err := reader.Read(headersBuf); err != nil {
		return nil, 0, err
	}
	var (
		offset = int64(proto.Encoding.Uint64(headersBuf[0:]))
		size   = proto.Encoding.Uint32(headersBuf[8:])
		buf    = make([]byte, int(size)+len(headersBuf))
		n      = copy(buf, headersBuf)
	)
	if _, err := reader.Read(buf[n:]); err != nil {
		return nil, 0, err
	}
	return buf, offset, nil

}

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
