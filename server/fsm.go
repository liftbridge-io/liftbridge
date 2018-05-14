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
	case proto.Op_CREATE_STREAM:
		stream := log.CreateStreamOp.Stream
		// Make sure to set the leader epoch on the stream.
		stream.LeaderEpoch = l.Term
		if err := s.createStream(stream); err != nil {
			panic(err)
		}
	case proto.Op_SHRINK_ISR:
		var (
			stream  = log.ShrinkISROp.Stream
			replica = log.ShrinkISROp.ReplicaToRemove
		)
		if err := s.shrinkISR(stream, replica); err != nil {
			panic(err)
		}
	case proto.Op_CHANGE_LEADER:
		stream := log.ChangeLeaderOp.Stream
		if err := s.changeStreamLeader(
			stream.Subject, stream.Name,
			log.ChangeLeaderOp.Leader, l.Term); err != nil {
			panic(err)
		}
	case proto.Op_EXPAND_ISR:
		var (
			stream  = log.ExpandISROp.Stream
			replica = log.ExpandISROp.ReplicaToAdd
		)
		if err := s.expandISR(stream, replica); err != nil {
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
	// TODO: return error.
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

	if stream.getLeader() == s.config.Clustering.ServerID {
		if err := stream.becomeLeader(); err != nil {
			return err
		}
	} else if stream.inReplicas(s.config.Clustering.ServerID) {
		if err := stream.becomeFollower(); err != nil {
			return err
		}
	}

	s.logger.Debugf("Created stream %s", stream)
	return nil
}

func (s *Server) shrinkISR(protoStream *proto.Stream, replica string) error {
	stream := s.metadata.GetStream(protoStream.Subject, protoStream.Name)
	if stream == nil {
		return fmt.Errorf("No such stream [subject=%s, name=%s]",
			protoStream.Subject, protoStream.Name)
	}
	if !stream.inReplicas(replica) {
		return fmt.Errorf("Cannot remove %s from stream %s ISR, not a replica", replica, stream)
	}
	stream.removeFromISR(replica)
	s.logger.Warnf("Removed replica %s from ISR for stream %s", replica, stream)
	return nil
}

func (s *Server) expandISR(protoStream *proto.Stream, replica string) error {
	stream := s.metadata.GetStream(protoStream.Subject, protoStream.Name)
	if stream == nil {
		return fmt.Errorf("No such stream [subject=%s, name=%s]",
			protoStream.Subject, protoStream.Name)
	}
	if !stream.inReplicas(replica) {
		return fmt.Errorf("Cannot add %s to stream %s ISR, not a replica", replica, stream)
	}
	stream.addToISR(replica)
	s.logger.Warnf("Added replica %s to ISR for stream %s", replica, stream)
	return nil
}

func (s *Server) changeStreamLeader(subject, name string, leader string, epoch uint64) error {
	stream := s.metadata.GetStream(subject, name)
	if stream == nil {
		return fmt.Errorf("No such stream [subject=%s, name=%s]", subject, name)
	}

	stream.setLeader(leader, epoch)

	if leader == s.config.Clustering.ServerID {
		if err := stream.becomeLeader(); err != nil {
			return err
		}
	} else if stream.inReplicas(s.config.Clustering.ServerID) {
		if err := stream.becomeFollower(); err != nil {
			return err
		}
	}

	s.logger.Debugf("Changed leader for stream %s to %s", stream, leader)
	return nil
}
