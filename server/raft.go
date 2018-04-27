package server

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/tylertreat/nats-on-a-log"

	"github.com/tylertreat/jetbridge/server/proto"
)

type raftLogger struct {
	*Server
}

func (r *raftLogger) Write(b []byte) (int, error) {
	if !r.raftLogging {
		return len(b), nil
	}
	levelStart := bytes.IndexByte(b, '[')
	if levelStart != -1 {
		switch b[levelStart+1] {
		case 'D': // [DEBUG]
			r.logger.Debugf("%s", b[levelStart+8:])
		case 'I': // [INFO]
			r.logger.Infof("%s", b[levelStart+7:])
		case 'W': // [WARN]
			r.logger.Warnf("%s", b[levelStart+7:])
		case 'E': // [ERR]
			r.logger.Errorf("%s", b[levelStart+6:])
		default:
			r.logger.Infof("%s", b)
		}
	}
	return len(b), nil
}

func (rl *raftLogger) Close() error { return nil }

// Apply applies a Raft log entry to the controller.
func (s *Server) Apply(l *raft.Log) interface{} {
	log := &proto.RaftLog{}
	if err := log.Unmarshal(l.Data); err != nil {
		panic(err)
	}
	switch log.Op {
	case proto.RaftLog_CREATE_STREAM:
		stream := log.CreateStreamOp.Stream
		if err := s.metadata.AddStream(stream); err != nil {
			panic(err)
		}
		s.logger.Infof("Added stream [subject=%s, name=%s, replicationFactor=%d]",
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

func (s *Server) setupRaft(name string) error {
	path := filepath.Join(s.config.RaftPath, name)

	// Configure Raft.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.config.NodeID)
	logWriter := &raftLogger{s}
	config.LogOutput = logWriter

	// Setup Raft communication.
	addr := s.getClusteringAddr(name)
	tr, err := natslog.NewNATSTransport(addr, s.nats, 2*time.Second, logWriter)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(path, s.config.RaftSnapshots, logWriter)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	cacheStore, err := raft.NewLogCache(s.config.RaftCacheSize, logStore)
	if err != nil {
		logStore.Close()
		return err
	}

	// Instantiate the Raft systems.
	raft, err := raft.NewRaft(config, s, cacheStore, logStore, snapshots, tr)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = raft

	// TODO: bootstrap cluster.

	return nil
}

func (s *Server) getClusteringAddr(raftName string) string {
	return s.getClusteringPeerAddr(raftName, s.config.NodeID)
}

func (s *Server) getClusteringPeerAddr(raftName, nodeID string) string {
	return fmt.Sprintf("%s.%s.%s", s.config.ClusterNamespace, raftName, nodeID)
}
