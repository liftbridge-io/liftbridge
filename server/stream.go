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
	client "github.com/tylertreat/go-liftbridge/proto"
	"golang.org/x/net/context"

	"github.com/tylertreat/liftbridge/server/commitlog"
	"github.com/tylertreat/liftbridge/server/proto"
)

// recvChannelSize specifies the size of the channel that feeds the leader
// message processing loop.
const recvChannelSize = 64 * 1024

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
	leaderLastSeen  time.Time
	leaderEpoch     uint64
	recovered       bool
	stopFollower    chan struct{}
	stopLeader      chan struct{}
}

func (s *Server) newStream(protoStream *proto.Stream, recovered bool) (*stream, error) {
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
		commitCheck: make(chan struct{}, len(protoStream.Replicas)),
		recovered:   recovered,
	}

	return st, nil
}

func (s *stream) String() string {
	return fmt.Sprintf("[subject=%s, name=%s]", s.Subject, s.Name)
}

func (s *stream) close() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isFollowing {
		if err := s.stopFollowing(); err != nil {
			return err
		}
	} else if s.isLeading {
		if err := s.stopLeading(); err != nil {
			return err
		}
	}

	if err := s.log.Close(); err != nil {
		return err
	}

	return nil
}

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

func (s *stream) startLeadingOrFollowing() error {
	if s.Leader == s.srv.config.Clustering.ServerID {
		s.srv.logger.Debugf("Server becoming leader for stream %s", s)
		if err := s.becomeLeader(s.LeaderEpoch); err != nil {
			s.srv.logger.Errorf("Server failed becoming leader for stream %s: %v", s, err)
			return err
		}
	} else if s.inReplicas(s.srv.config.Clustering.ServerID) {
		s.srv.logger.Debugf("Server becoming follower for stream %s", s)
		if err := s.becomeFollower(); err != nil {
			s.srv.logger.Errorf("Server failed becoming follower for stream %s: %v", s, err)
			return err
		}
	}
	return nil
}

func (s *stream) GetLeader() (string, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Leader, s.LeaderEpoch
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

	// Start message processing loop.
	s.recvChan = make(chan *nats.Msg, recvChannelSize)
	s.stopLeader = make(chan struct{})
	go s.messageProcessingLoop(s.recvChan, s.stopLeader)

	// Start replicating to followers.
	s.startReplicating(epoch, s.stopLeader)

	// Subscribe to the NATS subject and begin sequencing messages.
	sub, err := s.srv.nc.QueueSubscribe(s.Subject, s.ConsumerGroup, func(m *nats.Msg) {
		s.recvChan <- m
	})
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to NATS")
	}
	sub.SetPendingLimits(-1, -1)
	s.sub = sub

	// Also subscribe to the stream replication subject.
	sub, err = s.srv.ncRepl.Subscribe(s.getReplicationRequestInbox(), s.handleReplicationRequest)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to replication inbox")
	}
	sub.SetPendingLimits(-1, -1)
	s.leaderReplSub = sub

	s.isLeading = true
	s.isFollowing = false

	return nil
}

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
	close(s.stopLeader)
	s.commitQueue.Dispose()

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
	sub.SetPendingLimits(-1, -1)
	s.followerReplSub = sub

	// Start fetching messages from the leader's log starting at the HW.
	s.stopFollower = make(chan struct{})
	s.srv.logger.Debugf("Replicating stream %s from leader %s", s, s.Leader)
	go s.replicationRequestLoop(s.stopFollower)

	// Start leader failure detector.
	go s.leaderFailureDetectorLoop(s.stopFollower)

	s.isFollowing = true
	s.isLeading = false

	return nil
}

func (s *stream) stopFollowing() error {
	if err := s.followerReplSub.Unsubscribe(); err != nil {
		return err
	}

	// Stop replication request and leader failure detector loops.
	close(s.stopFollower)

	return nil
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
	// We should have at least 16 bytes, 8 for leader epoch and 8 for HW.
	if len(msg.Data) < 16 {
		s.srv.logger.Warnf("Invalid replication response for stream %s", s)
		return
	}

	leaderEpoch := proto.Encoding.Uint64(msg.Data[:8])

	s.mu.Lock()
	if s.LeaderEpoch != leaderEpoch {
		s.mu.Unlock()
		return
	}
	s.leaderLastSeen = time.Now()
	s.mu.Unlock()

	// Update HW from leader's HW.
	hw := int64(proto.Encoding.Uint64(msg.Data[8:]))
	s.log.SetHighWatermark(hw)

	data := msg.Data[16:]
	if len(data) == 0 {
		return
	}

	// We should have at least 12 bytes for headers.
	if len(data) <= 12 {
		s.srv.logger.Warnf("Invalid replication response for stream %s", s)
		return
	}
	offset := int64(proto.Encoding.Uint64(data[:8]))
	if offset == s.log.NewestOffset()+1 {
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

// TODO: I think there is potential for a race where this broker loses
// leadership for the stream while it's receiving messages here. Look into
// this.
func (s *stream) messageProcessingLoop(recvChan <-chan *nats.Msg, stop <-chan struct{}) {
	var (
		msg       *nats.Msg
		batchSize = s.srv.config.BatchMaxMessages
		batchWait = s.srv.config.BatchWaitTime
		msgBatch  = make([]*proto.Message, 0, batchSize)
		ackBatch  = make([]string, 0, batchSize)
	)
	for {
		msgBatch = msgBatch[:0]
		ackBatch = ackBatch[:0]
		select {
		case <-stop:
			return
		case msg = <-recvChan:
		}

		m, envelope := natsToProtoMessage(msg)
		msgBatch = append(msgBatch, m)
		if envelope != nil {
			ackBatch = append(ackBatch, envelope.AckInbox)
		} else {
			ackBatch = append(ackBatch, "")
		}
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
				m, envelope := natsToProtoMessage(msg)
				msgBatch = append(msgBatch, m)
				if envelope != nil {
					ackBatch = append(ackBatch, envelope.AckInbox)
				} else {
					ackBatch = append(ackBatch, "")
				}
			}
			remaining -= chanLen
		}

		// Write uncommitted messages to log.
		for i, msg := range msgBatch {
			ms := &proto.MessageSet{Messages: []*proto.Message{msg}}
			data, err := proto.Encode(ms)
			if err != nil {
				panic(err)
			}
			offset, err := s.log.Append(data)
			if err != nil {
				s.srv.logger.Errorf("Failed to append to log %s: %v", s, err)
				return
			}

			// Update this replica's latest offset.
			s.updateISRLatestOffset(s.srv.config.Clustering.ServerID, offset)

			// If there is an AckInbox, add the pending message to the commit
			// queue. If there isn't one, we don't care whether the message is
			// committed or not.
			ackInbox := ackBatch[i]
			if ackInbox != "" {
				if err := s.commitQueue.Put(&client.Ack{
					StreamSubject: s.Subject,
					StreamName:    s.Name,
					MsgSubject:    string(msg.Headers["subject"]),
					Offset:        offset,
					AckInbox:      ackInbox,
				}); err != nil {
					// This is very bad and should not happen.
					panic(fmt.Sprintf("Failed to add message to commit queue: %v", err))
				}
			}
		}
	}
}

func (s *stream) startReplicating(epoch uint64, stop chan struct{}) {
	if s.ReplicationFactor > 1 {
		s.srv.logger.Debugf("Replicating stream %s to followers", s)
	}
	s.commitQueue = queue.New(100)
	go s.commitLoop(stop)

	s.replicators = make(map[string]*replicator, len(s.replicas)-1)
	for replica, _ := range s.replicas {
		if replica == s.srv.config.Clustering.ServerID {
			// Don't replicate to ourselves.
			continue
		}
		r := &replicator{
			replica:           replica,
			stream:            s,
			requests:          make(chan *proto.ReplicationRequest),
			hw:                -1,
			maxLagTime:        s.srv.config.Clustering.ReplicaMaxLagTime,
			leader:            s.srv.config.Clustering.ServerID,
			cancelReplication: func() {}, // Intentional no-op initially
		}
		s.replicators[replica] = r
		go r.start(epoch, stop)
	}
}

func (s *stream) commitLoop(stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-s.commitCheck:
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
			s.srv.ncAcks.Publish(ack.AckInbox, data)
		}
	}
}

func (s *stream) replicationRequestLoop(stop chan struct{}) {
	ticker := time.NewTicker(s.srv.config.Clustering.ReplicaFetchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sendReplicationRequest()
		case <-stop:
			return
		}
	}
}

func (s *stream) leaderFailureDetectorLoop(stop chan struct{}) {
	s.mu.Lock()
	s.leaderLastSeen = time.Now()
	s.mu.Unlock()
	interval := s.srv.config.Clustering.ReplicaMaxLeaderTimeout / 2
	if interval == 0 {
		interval = s.srv.config.Clustering.ReplicaMaxLeaderTimeout
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var now time.Time
	for {
		select {
		case <-stop:
			return
		case now = <-ticker.C:
		}
		s.mu.RLock()
		var (
			leader          = s.Leader
			lastSeenElapsed = now.Sub(s.leaderLastSeen)
		)
		s.mu.RUnlock()

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
	// Add 1 because we don't want to truncate the HW itself.
	s.srv.logger.Debugf("Truncating log for stream %s to HW %d", s, hw+1)
	return s.log.Truncate(hw + 1)
}

func (s *stream) inISR(replica string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.isr[replica]
	return ok
}

func (s *stream) inReplicas(id string) bool {
	_, ok := s.replicas[id]
	return ok
}

func (s *stream) RemoveFromISR(replica string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inReplicas(replica) {
		return fmt.Errorf("%s not a replica", replica)
	}
	delete(s.isr, replica)
	return nil
}

func (s *stream) AddToISR(rep string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inReplicas(rep) {
		return fmt.Errorf("%s not a replica", rep)
	}
	s.isr[rep] = &replica{offset: -1}
	return nil
}

func (s *stream) GetEpoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Epoch
}

func (s *stream) SetEpoch(epoch uint64) {
	s.mu.Lock()
	s.Epoch = epoch
	s.mu.Unlock()
}

func (s *stream) Marshal() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := s.Stream.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func (s *stream) ISRSize() int {
	s.mu.RLock()
	size := len(s.isr)
	s.mu.RUnlock()
	return size
}

func (s *stream) GetISR() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	isr := make([]string, 0, len(s.isr))
	for replica, _ := range s.isr {
		isr = append(isr, replica)
	}
	return isr
}

func (s *stream) GetReplicas() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	replicas := make([]string, 0, len(s.replicas))
	for replica, _ := range s.replicas {
		replicas = append(replicas, replica)
	}
	return replicas
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

func natsToProtoMessage(msg *nats.Msg) (*proto.Message, *client.Message) {
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
	return m, envelope
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
