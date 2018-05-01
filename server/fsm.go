package server

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"

	"github.com/tylertreat/jetbridge/server/proto"
)

// Apply applies a Raft log entry to the controller FSM.
func (s *Server) Apply(l *raft.Log) interface{} {
	log := &proto.RaftLog{}
	if err := log.Unmarshal(l.Data); err != nil {
		panic(err)
	}
	switch log.Op {
	case proto.RaftLog_CREATE_STREAM:
		stream := log.CreateStreamOp.Stream
		if err := s.createStream(stream); err != nil {
			panic(err)
		}
		s.logger.Debugf("Applied stream create to Raft [subject=%s, name=%s, replicationFactor=%d]",
			stream.Subject, stream.Name, stream.ReplicationFactor)
	default:
		panic(fmt.Sprintf("Unknown Raft operation: %s", log.Op))
	}
	return nil
}

func (s *Server) Snapshot() (raft.FSMSnapshot, error) {
	// TODO
	return nil, nil
}

func (s *Server) Restore(r io.ReadCloser) error {
	// TODO
	return nil
}

func (s *Server) createStream(protoStream *proto.Stream) error {
	// Do idempotency check.
	s.mu.RLock()
	streams := s.streams[protoStream.Subject]
	if streams != nil {
		if _, ok := streams[protoStream.Name]; ok {
			s.mu.RUnlock()
			return nil
		}
	}
	s.mu.RUnlock()

	// TODO: do we need this store?
	if err := s.metadata.AddStream(protoStream); err != nil {
		return errors.Wrap(err, "failed to add stream to metadata store")
	}

	// This will initialize/recover the durable commit log.
	stream, err := s.newStream(protoStream)
	if err != nil {
		return errors.Wrap(err, "failed to create stream")
	}

	if stream.Leader == s.config.Clustering.NodeID {
		// If we are the stream leader, subscribe to the NATS subject and begin
		// sequencing messages.
		sub, err := s.nats.QueueSubscribe(stream.Subject, stream.ConsumerGroup, stream.handleMsg)
		if err != nil {
			return errors.Wrap(err, "failed to subscribe to NATS")
		}
		stream.sub = sub
	} else {
		// Otherwise, start fetching messages from the leader's log.
		// TODO
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	streams = s.streams[stream.Subject]
	if streams == nil {
		streams = make(subjectStreams)
		s.streams[stream.Subject] = streams
	}
	streams[stream.Name] = stream

	return nil
}
