package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/nats-io/nats.go"

	"github.com/liftbridge-io/nats-on-a-log"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

const (
	defaultJoinRaftGroupTimeout       = time.Second
	defaultRaftJoinAttempts           = 30
	defaultBootstrapMisconfigInterval = 10 * time.Second
)

var (
	raftJoinAttempts           = defaultRaftJoinAttempts
	bootstrapMisconfigInterval = defaultBootstrapMisconfigInterval
)

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
		r.Raft.Shutdown()
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

// raftLogger implements io.WriteCloser by piping data to the Server logger.
type raftLogger struct {
	*Server
}

// Write pipes the given data to the Server logger.
func (r *raftLogger) Write(b []byte) (int, error) {
	if !r.config.LogRaft {
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

// Close is a no-op to implement io.WriteCloser.
func (r *raftLogger) Close() error { return nil }

// setupMetadataRaft creates and starts an embedded Raft node for replicating
// cluster metadata. The node will load configuration from previous state, if
// there is any. If there isn't previous state, depending on server
// configuration, this will attempt to join an existing cluster, bootstrap as a
// seed node, or bootstrap using a predefined cluster configuration. If joining
// an existing cluster, this will attempt to join for up to 30 seconds before
// giving up and returning an error.
func (s *Server) setupMetadataRaft() error {
	existingState, err := s.createRaftNode()
	if err != nil {
		return err
	}
	node := s.getRaft()

	// Bootstrap if there is no previous state and we are starting this node as
	// a seed or a cluster configuration is provided.
	bootstrap := !existingState &&
		(s.config.Clustering.RaftBootstrapSeed || len(s.config.Clustering.RaftBootstrapPeers) > 0)
	if bootstrap {
		if err := s.bootstrapCluster(node.Raft); err != nil {
			node.shutdown()
			return err
		}
		s.logger.Debug("Successfully bootstrapped metadata Raft group")
	} else if !existingState {
		// Attempt to join the cluster if we're not bootstrapping.
		req, err := proto.MarshalRaftJoinRequest(&proto.RaftJoinRequest{
			NodeID:   s.config.Clustering.ServerID,
			NodeAddr: s.config.Clustering.ServerID, // NATS transport uses ID for addr.
		})
		if err != nil {
			panic(err)
		}
		joined := false
		// Attempt to join for up to 30 seconds before giving up.
		for i := 0; i < raftJoinAttempts; i++ {
			s.logger.Debug("Attempting to join metadata Raft group...")
			r, err := s.ncRaft.Request(fmt.Sprintf("%s.join", s.baseMetadataRaftSubject()),
				req, defaultJoinRaftGroupTimeout)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			resp, err := proto.UnmarshalRaftJoinResponse(r.Data)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			if resp.Error != "" {
				time.Sleep(time.Second)
				continue
			}
			joined = true
			break
		}
		if joined {
			s.logger.Debug("Successfully joined metadata Raft group")
		} else {
			node.shutdown()
			return errors.New("failed to join metadata Raft group")
		}
	}
	if s.config.Clustering.RaftBootstrapSeed {
		// If node is started with bootstrap, regardless if state exists or
		// not, try to detect (and report) other nodes in same cluster started
		// with bootstrap=true.
		s.startGoroutine(s.detectBootstrapMisconfig)
	}

	return nil
}

// bootstrapCluster bootstraps the node for the provided Raft group either as a
// seed node or with the given peer configuration, depending on configuration
// and with the latter taking precedence.
func (s *Server) bootstrapCluster(node *raft.Raft) error {
	// Include ourself in the cluster.
	servers := []raft.Server{{
		ID:      raft.ServerID(s.config.Clustering.ServerID),
		Address: raft.ServerAddress(s.config.Clustering.ServerID),
	}}
	if len(s.config.Clustering.RaftBootstrapPeers) > 0 {
		// Bootstrap using provided cluster configuration.
		s.logger.Debug("Bootstrapping metadata Raft group using provided configuration")
		for _, peer := range s.config.Clustering.RaftBootstrapPeers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),
				Address: raft.ServerAddress(peer), // NATS transport uses ID as addr.
			})
		}
	} else {
		// Bootstrap as a seed node.
		s.logger.Debug("Bootstrapping metadata Raft group as seed node")
	}
	config := raft.Configuration{Servers: servers}
	return node.BootstrapCluster(config).Error()
}

// detectBootstrapMisconfig attempts to detect if any other servers were
// started in bootstrap seed mode. If any are detected, the server will panic
// since this is a fatal state.
func (s *Server) detectBootstrapMisconfig() {
	srvID := []byte(s.config.Clustering.ServerID)
	subj := fmt.Sprintf("%s.bootstrap", s.baseMetadataRaftSubject())
	s.ncRaft.Subscribe(subj, func(m *nats.Msg) {
		if m.Data != nil {
			// Ignore message to ourself
			if string(m.Data) != s.config.Clustering.ServerID {
				m.Respond(srvID)
				s.logger.Fatalf("Server %s was also started with raft.bootstrap.seed", string(m.Data))
			}
		}
	})
	inbox := nats.NewInbox()
	s.ncRaft.Subscribe(inbox, func(m *nats.Msg) {
		s.logger.Fatalf("Server %s was also started with raft.bootstrap.seed", string(m.Data))
	})
	if err := s.ncRaft.Flush(); err != nil {
		s.logger.Errorf("Error setting up bootstrap misconfiguration detection: %v", err)
		return
	}
	ticker := time.NewTicker(bootstrapMisconfigInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ticker.C:
			s.ncRaft.PublishRequest(subj, inbox, srvID)
		}
	}
}

// createRaftNode creates and starts an embedded Raft node for replicating
// cluster metadata. It returns a bool indicating if the Raft node had existing
// state that was loaded.
func (s *Server) createRaftNode() (bool, error) {
	path := filepath.Join(s.config.DataDir, "raft")

	// Configure Raft.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.config.Clustering.ServerID)
	logWriter := &raftLogger{s}
	config.LogOutput = logWriter
	if s.config.Clustering.RaftSnapshotThreshold != 0 {
		config.SnapshotThreshold = s.config.Clustering.RaftSnapshotThreshold
	}

	// Setup a channel for reliable leader notifications.
	raftNotifyCh := make(chan bool, 1)
	config.NotifyCh = raftNotifyCh

	// Setup Raft communication.
	tr, err := natslog.NewNATSTransport(s.config.Clustering.ServerID, s.baseMetadataRaftSubject()+".",
		s.ncRaft, 2*time.Second, logWriter)
	if err != nil {
		return false, err
	}

	// Create the snapshot store. This allows Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(path, s.config.Clustering.RaftSnapshots, logWriter)
	if err != nil {
		tr.Close()
		return false, fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and cache.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		tr.Close()
		return false, fmt.Errorf("new bolt store: %s", err)
	}
	cacheStore, err := raft.NewLogCache(s.config.Clustering.RaftCacheSize, logStore)
	if err != nil {
		tr.Close()
		logStore.Close()
		return false, err
	}

	// Instantiate the Raft node.
	node, err := raft.NewRaft(config, s, cacheStore, logStore, snapshots, tr)
	if err != nil {
		tr.Close()
		logStore.Close()
		return false, fmt.Errorf("new raft: %s", err)
	}

	// Check if there is existing state.
	existingState, err := raft.HasExistingState(cacheStore, logStore, snapshots)
	if err != nil {
		node.Shutdown()
		tr.Close()
		logStore.Close()
		return false, err
	}

	if existingState {
		s.logger.Debug("Loaded existing state for metadata Raft group")
	}

	// Handle requests to join the cluster.
	subj := fmt.Sprintf("%s.join", s.baseMetadataRaftSubject())
	sub, err := s.ncRaft.Subscribe(subj, func(msg *nats.Msg) {
		// Drop the request if we're not the leader. There's no race condition
		// after this check because even if we proceed with the cluster add, it
		// will fail if the node is not the leader as cluster changes go
		// through the Raft log.
		if node.State() != raft.Leader {
			return
		}
		req, err := proto.UnmarshalRaftJoinRequest(msg.Data)
		if err != nil {
			s.logger.Warn("Invalid join request for metadata Raft group")
			return
		}

		// Add the node as a voter. This is idempotent. No-op if the request
		// came from ourselves.
		resp := &proto.RaftJoinResponse{}
		if req.NodeID != s.config.Clustering.ServerID {
			future := node.AddVoter(
				raft.ServerID(req.NodeID),
				raft.ServerAddress(req.NodeAddr), 0, 0)
			if err := future.Error(); err != nil {
				resp.Error = err.Error()
			}
		}

		// Send the response.
		r, err := proto.MarshalRaftJoinResponse(resp)
		if err != nil {
			panic(err)
		}
		msg.Respond(r)
	})
	if err != nil {
		node.Shutdown()
		tr.Close()
		logStore.Close()
		return false, err
	}

	s.setRaft(&raftNode{
		Raft:      node,
		store:     logStore,
		transport: tr,
		logInput:  logWriter,
		notifyCh:  raftNotifyCh,
		joinSub:   sub,
	})

	return existingState, nil
}

// baseMetadataRaftSubject returns the base NATS subject used for Raft-related
// operations.
func (s *Server) baseMetadataRaftSubject() string {
	return fmt.Sprintf("%s.raft.metadata", s.config.Clustering.Namespace)
}
