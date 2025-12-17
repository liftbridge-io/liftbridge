package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/nats-io/nats.go"

	natslog "github.com/liftbridge-io/nats-on-a-log"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

const (
	defaultJoinRaftGroupTimeout       = time.Second
	defaultRaftJoinAttempts           = 30
	defaultBootstrapMisconfigInterval = 10 * time.Second
	defaultRaftApplyTimeout           = 5 * time.Second
)

var (
	raftJoinAttempts           = defaultRaftJoinAttempts
	bootstrapMisconfigInterval = defaultBootstrapMisconfigInterval
)

// timeoutFuture wraps a raft.Future with a timeout on the call to Error().
// This is used as a workaround to an issue in the Hashicorp Raft library in
// which Raft operations can deadlock if a quorum cannot be reached (which can
// be indefinitely, e.g. in the case of shutting down the cluster). See this
// issue: https://github.com/hashicorp/raft/issues/498.
type timeoutFuture struct {
	deadline  time.Time
	wrapped   raft.Future
	mu        sync.RWMutex
	responded bool
	err       error
}

// newTimeoutFuture returns a raft.ApplyFuture wrapping the provided
// raft.Future whose call to Error() will timeout at the given deadline.
func newTimeoutFuture(deadline time.Time, wrapped raft.Future) raft.ApplyFuture {
	return &timeoutFuture{
		deadline: deadline,
		wrapped:  wrapped,
	}
}

func (t *timeoutFuture) Error() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.responded {
		return t.err
	}

	errC := make(chan error)
	go func() {
		err := t.wrapped.Error()
		select {
		case errC <- err:
		default:
		}
	}()

	var err error
	select {
	case e := <-errC:
		err = e
	case <-time.After(time.Until(t.deadline)):
		err = errors.New("raft operation timed out")
	}

	t.responded = true
	t.err = err
	return err
}

func (t *timeoutFuture) Response() interface{} {
	if applyFuture, ok := t.wrapped.(raft.ApplyFuture); ok {
		return applyFuture.Response()
	}
	return nil
}

func (t *timeoutFuture) Index() uint64 {
	if applyFuture, ok := t.wrapped.(raft.ApplyFuture); ok {
		return applyFuture.Index()
	}
	return 0
}

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

// isLeader indicates if the Raft node is currently the leader.
func (r *raftNode) isLeader() bool {
	return atomic.LoadInt64(&(r.leader)) == 1
}

// setLeader sets the Raft node as the current leader or as a follower.
func (r *raftNode) setLeader(leader bool) {
	var flag int64
	if leader {
		flag = 1
	}
	atomic.StoreInt64(&(r.leader), flag)
}

// applyOperation proposes the given operation to the Raft cluster. This should
// only be called when the server is metadata leader. However, if the server
// has lost leadership, the returned future will yield an error. This will use
// the deadline provided on the context and check for preconditions using the
// supplied function, if provided. This will only return an error if
// preconditions have failed, indicating the operation was not proposed to the
// Raft cluster.
func (r *raftNode) applyOperation(ctx context.Context, op *proto.RaftLog,
	checkPreconditions func(*proto.RaftLog) error) (raft.ApplyFuture, error) {

	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}

	// We will acquire the mutex to prevent Raft operations from interleaving
	// such that preconditions can be validated.
	r.Lock()
	defer r.Unlock()

	deadline := computeDeadline(ctx)

	if checkPreconditions != nil {
		// Ensure the FSM is up to date by issuing a barrier.
		barrierFuture := newTimeoutFuture(deadline, r.Barrier(time.Until(deadline)))
		if err := barrierFuture.Error(); err != nil {
			return barrierFuture, nil
		}

		// Check that the FSM preconditions are valid before performing the
		// Raft operation.
		if err := checkPreconditions(op); err != nil {
			return nil, err
		}
	}

	// Apply the Raft Operation.
	return newTimeoutFuture(deadline, r.Apply(data, time.Until(deadline))), nil
}

// getCommitIndex returns the latest committed Raft index.
func (r *raftNode) getCommitIndex() uint64 {
	idx, err := strconv.ParseUint(r.Stats()["commit_index"], 10, 64)
	if err != nil {
		panic(err)
	}
	return idx
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
func (s *Server) setupMetadataRaft() (*raftNode, error) {
	node, existingState, err := s.createRaftNode()
	if err != nil {
		return nil, err
	}

	// Bootstrap if there is no previous state and we are starting this node as
	// a seed or a cluster configuration is provided.
	bootstrap := !existingState &&
		(s.config.Clustering.RaftBootstrapSeed || len(s.config.Clustering.RaftBootstrapPeers) > 0)
	if bootstrap {
		if err := s.bootstrapCluster(node.Raft); err != nil {
			node.shutdown()
			return nil, err
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
			return nil, errors.New("failed to join metadata Raft group")
		}
	}
	if s.config.Clustering.RaftBootstrapSeed {
		// If node is started with bootstrap, regardless if state exists or
		// not, try to detect (and report) other nodes in same cluster started
		// with bootstrap=true.
		s.startGoroutine(s.detectBootstrapMisconfig)
	}

	return node, nil
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
			if peer == s.config.Clustering.ServerID {
				// Don't add ourselves twice.
				continue
			}
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),
				Address: raft.ServerAddress(peer), // NATS transport uses ID as addr.
			})
		}
	} else {
		// Bootstrap as a seed node.
		s.logger.Debug("Bootstrapping metadata Raft group as seed node")
	}

	// Enforce quorum size limit. Any servers beyond the limit are non-voters.
	// However, the local server must always be a voter to bootstrap successfully
	// (required by raft v1.7.3+).
	maxQuorum := s.config.Clustering.RaftMaxQuorumSize
	if maxQuorum == 0 || maxQuorum > uint(len(servers)) {
		maxQuorum = uint(len(servers))
	}
	localID := raft.ServerID(s.config.Clustering.ServerID)

	// Sort servers but ensure local server is placed in the voter range.
	// First, sort all servers by ID.
	sort.SliceStable(servers, func(i, j int) bool { return servers[i].ID < servers[j].ID })

	// Find where local server is in the sorted list.
	localIdx := -1
	for i, server := range servers {
		if server.ID == localID {
			localIdx = i
			break
		}
	}

	// If local server would be a non-voter (beyond maxQuorum), swap it with the
	// last voter to ensure it can bootstrap.
	if localIdx >= int(maxQuorum) && maxQuorum > 0 {
		lastVoterIdx := int(maxQuorum) - 1
		servers[localIdx], servers[lastVoterIdx] = servers[lastVoterIdx], servers[localIdx]
	}

	// Mark servers beyond maxQuorum as non-voters.
	for i := int(maxQuorum); i < len(servers); i++ {
		servers[i] = raft.Server{
			ID:       servers[i].ID,
			Address:  servers[i].Address,
			Suffrage: raft.Nonvoter,
		}
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
	inbox := fmt.Sprintf("%s.bootstrap.reply", s.baseMetadataRaftSubject())
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
			if err := s.ncRaft.PublishRequest(subj, inbox, srvID); err != nil {
				s.logger.Errorf("Error publishing bootstrap misconfiguration detection message: %v", err)
			}
		}
	}
}

// createRaftNode creates and starts an embedded Raft node for replicating
// cluster metadata. It returns a bool indicating if the Raft node had existing
// state that was loaded.
func (s *Server) createRaftNode() (*raftNode, bool, error) {
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
		return nil, false, err
	}

	// Create the snapshot store. This allows Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(path, s.config.Clustering.RaftSnapshots, logWriter)
	if err != nil {
		tr.Close()
		return nil, false, fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and cache.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		tr.Close()
		return nil, false, fmt.Errorf("new bolt store: %s", err)
	}
	cacheStore, err := raft.NewLogCache(s.config.Clustering.RaftCacheSize, logStore)
	if err != nil {
		tr.Close()
		logStore.Close()
		return nil, false, err
	}

	// Instantiate the Raft node.
	node, err := raft.NewRaft(config, s, cacheStore, logStore, snapshots, tr)
	if err != nil {
		tr.Close()
		logStore.Close()
		return nil, false, fmt.Errorf("new raft: %s", err)
	}

	// Check if there is existing state.
	existingState, err := raft.HasExistingState(cacheStore, logStore, snapshots)
	if err != nil {
		node.Shutdown()
		tr.Close()
		logStore.Close()
		return nil, false, err
	}

	if existingState {
		s.logger.Debug("Loaded existing state for metadata Raft group")
	}

	// Handle requests to join the cluster.
	subj := fmt.Sprintf("%s.join", s.baseMetadataRaftSubject())
	sub, err := s.ncRaft.Subscribe(subj, s.newClusterJoinRequestHandler(node))
	if err != nil {
		node.Shutdown()
		tr.Close()
		logStore.Close()
		return nil, false, err
	}

	raftNode := &raftNode{
		Raft:      node,
		store:     logStore,
		transport: tr,
		logInput:  logWriter,
		notifyCh:  raftNotifyCh,
		joinSub:   sub,
	}
	s.setRaft(raftNode)

	return raftNode, existingState, nil
}

// newClusterJoinRequestHandler creates a NATS handler for handling requests
// to join the Raft cluster.
func (s *Server) newClusterJoinRequestHandler(node *raft.Raft) func(*nats.Msg) {
	return func(msg *nats.Msg) {
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

		resp := &proto.RaftJoinResponse{}

		// No-op if the request came from ourselves.
		if req.NodeID == s.config.Clustering.ServerID {
			r, err := proto.MarshalRaftJoinResponse(resp)
			if err != nil {
				panic(err)
			}
			msg.Respond(r)
			return
		}

		// Add the node to the cluster with appropriate suffrage. This is
		// idempotent.
		isVoter, err := s.addAsVoter(node)
		if err != nil {
			resp.Error = err.Error()
		} else {
			var future raft.IndexFuture
			if isVoter {
				s.logger.Debugf("Adding server %s to metadata Raft group as voter", req.NodeID)
				future = node.AddVoter(
					raft.ServerID(req.NodeID),
					raft.ServerAddress(req.NodeAddr), 0, 0)
			} else {
				s.logger.Debugf("Adding server %s to metadata Raft group as non-voter", req.NodeID)
				future = node.AddNonvoter(
					raft.ServerID(req.NodeID),
					raft.ServerAddress(req.NodeAddr), 0, 0)
			}
			if err := future.Error(); err != nil {
				resp.Error = err.Error()
			}
		}

		if resp.Error != "" {
			s.logger.Errorf("Failed to add server %s to metadata Raft group: %s", req.NodeID, resp.Error)
		}

		// Send the response.
		r, err := proto.MarshalRaftJoinResponse(resp)
		if err != nil {
			panic(err)
		}
		msg.Respond(r)
	}
}

// addAsVoter returns a bool indicating if a new node to be added to the
// cluster should be added as a voter or not based on current configuration. If
// we are below the max quorum size or there is no quorum limit, the new node
// will be added as a voter.  Otherwise, it's added as a non-voter.
func (s *Server) addAsVoter(node *raft.Raft) (bool, error) {
	maxQuorum := s.config.Clustering.RaftMaxQuorumSize
	if maxQuorum == 0 {
		return true, nil
	}
	// If there is a quorum limit, count the number of voting members.
	future := node.GetConfiguration()
	if err := future.Error(); err != nil {
		return false, err
	}
	voters := uint(0)
	for _, server := range future.Configuration().Servers {
		if server.Suffrage == raft.Voter || server.Suffrage == raft.Staging {
			voters++
		}
	}

	return voters < maxQuorum, nil
}

// baseMetadataRaftSubject returns the base NATS subject used for Raft-related
// operations.
func (s *Server) baseMetadataRaftSubject() string {
	return fmt.Sprintf("%s.raft.metadata", s.config.Clustering.Namespace)
}

func computeDeadline(ctx context.Context) time.Time {
	deadline, ok := ctx.Deadline()
	if ok {
		return deadline
	}
	return time.Now().Add(defaultRaftApplyTimeout)
}
