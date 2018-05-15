package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"

	"github.com/tylertreat/jetbridge/server/proto"
)

func (s *Server) recoverLatestCommittedFSMLog(applyIndex uint64) (*raft.Log, error) {
	commitIndex, err := strconv.ParseUint(s.raft.Stats()["commit_index"], 10, 64)
	if err != nil {
		return nil, err
	}
	firstIndex, err := s.raft.store.FirstIndex()
	if err != nil {
		return nil, err
	}
	if firstIndex == 0 {
		// No entries.
		return nil, nil
	}
	log := &raft.Log{}
	for i := commitIndex; i >= firstIndex; i-- {
		if i == applyIndex {
			// We are committing the first FSM log.
			return nil, nil
		}
		if err := s.raft.store.GetLog(i, log); err != nil {
			return nil, err
		}
		if log.Type == raft.LogCommand {
			return log, nil
		}
	}
	return nil, nil
}

// Apply applies a Raft log entry to the controller FSM.
func (s *Server) Apply(l *raft.Log) interface{} {
	if !s.startedRecovery {
		lastCommittedLog, err := s.recoverLatestCommittedFSMLog(l.Index)
		if err != nil {
			panic(err)
		}
		s.latestRecoveredLog = lastCommittedLog
		s.startedRecovery = true
		if s.latestRecoveredLog != nil {
			s.logger.Debug("Replaying Raft log...")
		}
	}

	recovered := false
	if s.latestRecoveredLog != nil && l.Index <= s.latestRecoveredLog.Index {
		recovered = true
		if l.Index == s.latestRecoveredLog.Index {
			defer func() {
				count, err := s.finishedRecovery()
				if err != nil {
					panic(fmt.Sprintf("failed to recover from Raft log: %v", err))
				}
				s.logger.Debugf("Finished replaying Raft log, recovered %d streams", count)
			}()
			s.latestRecoveredLog = nil
		}
	}

	log := &proto.RaftLog{}
	if err := log.Unmarshal(l.Data); err != nil {
		panic(err)
	}
	switch log.Op {
	case proto.Op_CREATE_STREAM:
		stream := log.CreateStreamOp.Stream
		// Make sure to set the leader epoch on the stream.
		stream.LeaderEpoch = l.Index
		stream.Epoch = l.Index
		err := s.applyCreateStream(stream, recovered)
		if err == ErrStreamExists {
			return err
		}
		if err != nil {
			panic(err)
		}
	case proto.Op_SHRINK_ISR:
		var (
			subject = log.ShrinkISROp.Subject
			name    = log.ShrinkISROp.Name
			replica = log.ShrinkISROp.ReplicaToRemove
		)
		if err := s.applyShrinkISR(subject, name, replica, l.Index); err != nil {
			panic(err)
		}
	case proto.Op_CHANGE_LEADER:
		var (
			subject = log.ChangeLeaderOp.Subject
			name    = log.ChangeLeaderOp.Name
			leader  = log.ChangeLeaderOp.Leader
		)
		if err := s.applyChangeStreamLeader(subject, name, leader, l.Index); err != nil {
			panic(err)
		}
	case proto.Op_EXPAND_ISR:
		var (
			subject = log.ExpandISROp.Subject
			name    = log.ExpandISROp.Name
			replica = log.ExpandISROp.ReplicaToAdd
		)
		if err := s.applyExpandISR(subject, name, replica, l.Index); err != nil {
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
	s.logger.Debug("Restoring Raft state from snapshot...")
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
	count := 0
	for _, stream := range snap.Streams {
		if err := s.applyCreateStream(stream, false); err != nil {
			return err
		}
		count++
	}
	s.logger.Debugf("Finished restoring Raft state from snapshot, recovered %d streams", count)
	return nil
}

func (s *Server) applyCreateStream(protoStream *proto.Stream, recovered bool) error {
	stream, err := s.metadata.AddStream(protoStream, recovered)
	if err == ErrStreamExists {
		return err
	}
	if err != nil {
		return errors.Wrap(err, "failed to add stream to metadata store")
	}
	s.logger.Debugf("fsm: Created stream %s", stream)
	return nil
}

func (s *Server) applyShrinkISR(subject, name, replica string, epoch uint64) error {
	stream := s.metadata.GetStream(subject, name)
	if stream == nil {
		return fmt.Errorf("No such stream [subject=%s, name=%s]", subject, name)
	}

	// Idempotency check.
	if stream.GetEpoch() >= epoch {
		return nil
	}

	if err := stream.RemoveFromISR(replica); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to remove %s from ISR for stream %s",
			replica, stream))
	}

	stream.SetEpoch(epoch)

	s.logger.Warnf("fsm: Removed replica %s from ISR for stream %s", replica, stream)
	return nil
}

func (s *Server) applyExpandISR(subject, name, replica string, epoch uint64) error {
	stream := s.metadata.GetStream(subject, name)
	if stream == nil {
		return fmt.Errorf("No such stream [subject=%s, name=%s]", subject, name)
	}

	// Idempotency check.
	if stream.GetEpoch() >= epoch {
		return nil
	}

	if err := stream.AddToISR(replica); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to add %s to ISR for stream %s",
			replica, stream))
	}

	stream.SetEpoch(epoch)

	s.logger.Warnf("fsm: Added replica %s to ISR for stream %s", replica, stream)
	return nil
}

func (s *Server) applyChangeStreamLeader(subject, name string, leader string, epoch uint64) error {
	stream := s.metadata.GetStream(subject, name)
	if stream == nil {
		return fmt.Errorf("No such stream [subject=%s, name=%s]", subject, name)
	}

	// Idempotency check.
	if stream.GetEpoch() >= epoch {
		return nil
	}

	if err := stream.SetLeader(leader, epoch); err != nil {
		return errors.Wrap(err, "failed to change stream leader")
	}

	stream.SetEpoch(epoch)

	s.logger.Debugf("fsm: Changed leader for stream %s to %s", stream, leader)
	return nil
}
