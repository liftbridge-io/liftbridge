package server

import (
	"encoding/binary"
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
	default:
		panic(fmt.Sprintf("Unknown Raft operation: %s", log.Op))
	}
	return nil
}

type fsmSnapshot struct {
	*proto.MetadataSnapshot
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := f.Marshal()
		if err != nil {
			return err
		}

		// Write size and data to sink.
		sizeBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBuf, uint32(len(b)))
		if _, err := sink.Write(sizeBuf); err != nil {
			return err
		}
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}

func (s *Server) Snapshot() (raft.FSMSnapshot, error) {
	var (
		streams = s.metadata.GetStreams()
		protos  = make([]*proto.Stream, len(streams))
	)
	for i, stream := range streams {
		protos[i] = stream.Stream
	}
	return &fsmSnapshot{&proto.MetadataSnapshot{Streams: protos}}, nil
}

func (s *Server) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	// Read snapshot size.
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(snapshot, sizeBuf); err != nil {
		return err
	}
	// Read snapshot.
	size := binary.BigEndian.Uint32(sizeBuf)
	buf := make([]byte, size)
	if _, err := io.ReadFull(snapshot, buf); err != nil {
		return err
	}
	snap := &proto.MetadataSnapshot{}
	if err := snap.Unmarshal(buf); err != nil {
		return err
	}

	// Drop state and restore.
	if err := s.metadata.Reset(); err != nil {
		return err
	}
	for _, stream := range snap.Streams {
		if err := s.createStream(stream); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) createStream(protoStream *proto.Stream) error {
	// Do idempotency check.
	if stream := s.metadata.GetStream(protoStream.Subject, protoStream.Name); stream != nil {
		return nil
	}

	// This will initialize/recover the durable commit log.
	stream, err := s.newStream(protoStream)
	if err != nil {
		return errors.Wrap(err, "failed to create stream")
	}

	if err := s.metadata.AddStream(stream); err != nil {
		return errors.Wrap(err, "failed to add stream to metadata store")
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

	s.logger.Debugf("Created stream %s", stream)
	return nil
}
