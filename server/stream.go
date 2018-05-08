package server

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"

	client "github.com/tylertreat/go-jetbridge/proto"

	"github.com/tylertreat/jetbridge/server/commitlog"
	"github.com/tylertreat/jetbridge/server/proto"
)

var envelopeCookie = []byte("jetb")

type stream struct {
	*proto.Stream
	mu                    sync.RWMutex
	sub                   *nats.Subscription // Subscription to stream NATS subject
	replSub               *nats.Subscription // Subscription for replication requests
	log                   CommitLog
	srv                   *Server
	subjectHash           string
	replicating           bool
	requestingReplication bool
	replicas              map[string]struct{}
	isr                   map[string]struct{}
	replicators           map[string]*replicator
}

func (s *Server) newStream(protoStream *proto.Stream) (*stream, error) {
	log, err := commitlog.New(commitlog.Options{
		Path:            filepath.Join(s.config.Clustering.RaftPath, protoStream.Subject, protoStream.Name),
		MaxSegmentBytes: s.config.Log.SegmentMaxBytes,
		MaxLogBytes:     s.config.Log.RetentionMaxBytes,
		Compact:         s.config.Log.Compact,
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

	isr := make(map[string]struct{}, len(protoStream.Isr))
	for _, replica := range protoStream.Isr {
		isr[replica] = struct{}{}
	}

	st := &stream{
		Stream:      protoStream,
		log:         log,
		srv:         s,
		subjectHash: subjectHash,
		replicas:    replicas,
		isr:         isr,
		replicators: make(map[string]*replicator, len(protoStream.Replicas)),
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
	if s.replSub != nil {
		return s.replSub.Unsubscribe()
	}
	return nil
}

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
	if msg.Reply != "" {
		m.Headers["reply"] = []byte(msg.Reply)
	}

	ms := &proto.MessageSet{Messages: []*proto.Message{m}}
	data, err := proto.Encode(ms)
	if err != nil {
		panic(err)
	}
	offset, err := s.log.Append(data)
	if err != nil {
		s.srv.logger.Errorf("Failed to append to log %s: %v", s, err)
		return
	}

	// Publish ack.
	if envelope != nil && envelope.AckInbox != "" {
		ack := &client.Ack{
			StreamSubject: s.Subject,
			StreamName:    s.Name,
			MsgSubject:    msg.Subject,
			Offset:        offset,
		}
		data, err := ack.Marshal()
		if err != nil {
			panic(err)
		}
		s.srv.nc.Publish(envelope.AckInbox, data)
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
	if len(msg.Data) <= 12 {
		s.srv.logger.Warnf("Invalid replication response for stream %s", s)
		return
	}
	offset := int64(proto.Encoding.Uint64(msg.Data[:8]))
	if offset > s.log.NewestOffset() {
		if _, err := s.log.Append(msg.Data); err != nil {
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

func (s *stream) startReplicating() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replicating {
		return
	}
	s.replicating = true
	s.srv.logger.Debugf("Replicating stream %s to followers", s)
	for _, replicator := range s.replicators {
		go replicator.start()
	}
}

func (s *stream) stopReplicating() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.replicating {
		return
	}
	s.replicating = false
	for _, replicator := range s.replicators {
		replicator.stop()
	}
}

func (s *stream) startReplicationRequests() {
	s.mu.Lock()
	if s.requestingReplication {
		s.mu.Unlock()
		return
	}
	s.requestingReplication = true
	s.mu.Unlock()
	s.srv.logger.Debugf("Replicating stream %s from leader %s", s, s.Leader)
	ticker := time.NewTicker(s.srv.config.Clustering.ReplicaFetchInterval)
	defer ticker.Stop()
	for {
		<-ticker.C
		s.mu.RLock()
		if !s.requestingReplication {
			s.mu.RUnlock()
			return
		}
		s.sendReplicationRequest()
	}
}

func (s *stream) stopReplicationRequests() {
	s.mu.Lock()
	s.requestingReplication = false
	s.mu.Unlock()
}

func (s *stream) sendReplicationRequest() {
	// TODO: this needs to be newest committed offset.
	data, err := (&proto.ReplicationRequest{
		ReplicaID:     s.srv.config.Clustering.ServerID,
		HighWatermark: s.log.NewestOffset(),
		Inbox:         s.getReplicationResponseInbox(),
	}).Marshal()
	if err != nil {
		panic(err)
	}
	s.srv.ncRepl.Publish(s.getReplicationRequestInbox(), data)
}

func (s *stream) inISR(replica string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.isr[replica]
	return ok
}

func (s *stream) removeFromISR(replica string) {
	s.mu.Lock()
	delete(s.isr, replica)
	s.mu.Unlock()
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
