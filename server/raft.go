package server

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/nats-io/go-nats"
	"github.com/tylertreat/nats-on-a-log"

	"github.com/tylertreat/jetbridge/server/proto"
)

const defaultJoinRaftGroupTimeout = time.Second

var joinRaftGroupTimeout = defaultJoinRaftGroupTimeout

// raftNode is a handle to a member in a Raft consensus group.
type raftNode struct {
	leader int64
	sync.Mutex
	closed bool
	*raft.Raft
	store     *raftboltdb.BoltStore
	transport *raft.NetworkTransport
	logInput  io.WriteCloser
	joinSub   *nats.Subscription
	notifyCh  <-chan bool
}

// shutdown attempts to stop the Raft node.
func (r *raftNode) shutdown() error {
	r.Lock()
	if r.closed {
		r.Unlock()
		return nil
	}
	r.closed = true
	r.Unlock()
	if r.Raft != nil {
		if err := r.Raft.Shutdown().Error(); err != nil {
			return err
		}
	}
	if r.transport != nil {
		if err := r.transport.Close(); err != nil {
			return err
		}
	}
	if r.store != nil {
		if err := r.store.Close(); err != nil {
			return err
		}
	}
	if r.joinSub != nil {
		if err := r.joinSub.Unsubscribe(); err != nil {
			return err
		}
	}
	if r.logInput != nil {
		if err := r.logInput.Close(); err != nil {
			return err
		}
	}
	return nil
}

type raftLogger struct {
	*Server
}

func (r *raftLogger) Write(b []byte) (int, error) {
	if !r.config.Clustering.RaftLogging {
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

func (s *Server) setupMetadataRaft() error {
	var (
		name               = "_metadata"
		addr               = s.getClusteringAddr(name)
		existingState, err = s.createRaftNode(name)
	)
	if err != nil {
		return err
	}
	node := s.raft

	// Bootstrap if there is no previous state and we are starting this node as
	// a seed or a cluster configuration is provided.
	bootstrap := !existingState && (s.config.Clustering.Bootstrap || len(s.config.Clustering.BootstrapPeers) > 0)
	if bootstrap {
		if err := s.bootstrapCluster(name, node.Raft); err != nil {
			node.shutdown()
			return err
		}
	} else if !existingState {
		// Attempt to join the cluster if we're not bootstrapping.
		req, err := (&proto.RaftJoinRequest{NodeID: s.config.Clustering.NodeID, NodeAddr: addr}).Marshal()
		if err != nil {
			panic(err)
		}
		var (
			joined = false
			resp   = &proto.RaftJoinResponse{}
		)
		s.logger.Debugf("Joining Raft group %s", name)
		// Attempt to join up to 5 times before giving up.
		for i := 0; i < 5; i++ {
			r, err := s.raftNats.Request(fmt.Sprintf("%s.%s.join", s.config.Clustering.Namespace, name),
				req, joinRaftGroupTimeout)
			if err != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			if err := resp.Unmarshal(r.Data); err != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			if resp.Error != "" {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			joined = true
			break
		}
		if !joined {
			node.shutdown()
			return fmt.Errorf("failed to join Raft group %s", name)
		}
	}
	if s.config.Clustering.Bootstrap {
		// If node is started with bootstrap, regardless if state exists or
		// not, try to detect (and report) other nodes in same cluster started
		// with bootstrap=true.
		go s.detectBootstrapMisconfig(name)
	}

	return nil
}

// bootstrapCluster bootstraps the node for the provided Raft group either as a
// seed node or with the given peer configuration, depending on configuration
// and with the latter taking precedence.
func (s *Server) bootstrapCluster(name string, node *raft.Raft) error {
	var (
		addr = s.getClusteringAddr(name)
		// Include ourself in the cluster.
		servers = []raft.Server{raft.Server{
			ID:      raft.ServerID(s.config.Clustering.NodeID),
			Address: raft.ServerAddress(addr),
		}}
	)
	if len(s.config.Clustering.BootstrapPeers) > 0 {
		// Bootstrap using provided cluster configuration.
		s.logger.Debugf("Bootstrapping Raft group %s using provided configuration", name)
		for _, peer := range s.config.Clustering.BootstrapPeers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),
				Address: raft.ServerAddress(s.getClusteringPeerAddr(name, peer)),
			})
		}
	} else {
		// Bootstrap as a seed node.
		s.logger.Debugf("Bootstrapping Raft group %s as seed node", name)
	}
	config := raft.Configuration{Servers: servers}
	return node.BootstrapCluster(config).Error()
}

func (s *Server) detectBootstrapMisconfig(name string) {
	srvID := []byte(s.config.Clustering.NodeID)
	subj := fmt.Sprintf("%s.%s.bootstrap", s.config.Clustering.Namespace, name)
	s.raftNats.Subscribe(subj, func(m *nats.Msg) {
		if m.Data != nil && m.Reply != "" {
			// Ignore message to ourself
			if string(m.Data) != s.config.Clustering.NodeID {
				s.raftNats.Publish(m.Reply, srvID)
				s.logger.Fatalf("Server %s was also started with -cluster_bootstrap", string(m.Data))
			}
		}
	})
	inbox := nats.NewInbox()
	s.raftNats.Subscribe(inbox, func(m *nats.Msg) {
		s.logger.Fatalf("Server %s was also started with -cluster_bootstrap", string(m.Data))
	})
	if err := s.raftNats.Flush(); err != nil {
		s.logger.Errorf("Error setting up bootstrap misconfiguration detection: %v", err)
		return
	}
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-s.shutdownCh:
			ticker.Stop()
			return
		case <-ticker.C:
			s.raftNats.PublishRequest(subj, inbox, srvID)
		}
	}
}

func (s *Server) createRaftNode(name string) (bool, error) {
	path := filepath.Join(s.config.Clustering.RaftPath, name)

	// Configure Raft.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.config.Clustering.NodeID)
	logWriter := &raftLogger{s}
	config.LogOutput = logWriter

	// Setup a channel for reliable leader notifications.
	raftNotifyCh := make(chan bool, 1)
	config.NotifyCh = raftNotifyCh

	// Setup Raft communication.
	addr := s.getClusteringAddr(name)
	tr, err := natslog.NewNATSTransport(addr, s.raftNats, 2*time.Second, logWriter)
	if err != nil {
		return false, err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(path, s.config.Clustering.RaftSnapshots, logWriter)
	if err != nil {
		return false, fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		return false, fmt.Errorf("new bolt store: %s", err)
	}
	cacheStore, err := raft.NewLogCache(s.config.Clustering.RaftCacheSize, logStore)
	if err != nil {
		logStore.Close()
		return false, err
	}

	// Instantiate the Raft node.
	node, err := raft.NewRaft(config, s, cacheStore, logStore, snapshots, tr)
	if err != nil {
		return false, fmt.Errorf("new raft: %s", err)
	}

	existingState, err := raft.HasExistingState(cacheStore, logStore, snapshots)
	if err != nil {
		node.Shutdown()
		tr.Close()
		logStore.Close()
		return false, err
	}

	if existingState {
		s.logger.Debugf("Loaded existing state for Raft group %s", name)
	}

	// Handle requests to join the cluster.
	subj := fmt.Sprintf("%s.%s.join", s.config.Clustering.Namespace, name)
	sub, err := s.raftNats.Subscribe(subj, func(msg *nats.Msg) {
		// Drop the request if we're not the leader. There's no race condition
		// after this check because even if we proceed with the cluster add, it
		// will fail if the node is not the leader as cluster changes go
		// through the Raft log.
		if node.State() != raft.Leader {
			return
		}
		req := &proto.RaftJoinRequest{}
		if err := req.Unmarshal(msg.Data); err != nil {
			s.logger.Errorf("Invalid join request for Raft group %s", name)
			return
		}

		// Add the node as a voter. This is idempotent. No-op if the request
		// came from ourselves.
		resp := &proto.RaftJoinResponse{}
		if req.NodeID != s.config.Clustering.NodeID {
			future := node.AddVoter(
				raft.ServerID(req.NodeID),
				raft.ServerAddress(req.NodeAddr), 0, 0)
			if err := future.Error(); err != nil {
				resp.Error = err.Error()
			}
		}

		// Send the response.
		r, err := resp.Marshal()
		if err != nil {
			panic(err)
		}
		s.raftNats.Publish(msg.Reply, r)
	})
	if err != nil {
		node.Shutdown()
		tr.Close()
		logStore.Close()
		return false, err
	}

	s.raft = &raftNode{
		Raft:      node,
		store:     logStore,
		transport: tr,
		logInput:  logWriter,
		notifyCh:  raftNotifyCh,
		joinSub:   sub,
	}

	return existingState, nil
}

func (s *Server) getClusteringAddr(raftName string) string {
	return s.getClusteringPeerAddr(raftName, s.config.Clustering.NodeID)
}

func (s *Server) getClusteringPeerAddr(raftName, nodeID string) string {
	return fmt.Sprintf("%s.%s.%s", s.config.Clustering.Namespace, raftName, nodeID)
}
