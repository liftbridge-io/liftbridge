package server

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	client "github.com/liftbridge-io/liftbridge-grpc/go"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/liftbridge-io/liftbridge/server/logger"
	"github.com/liftbridge-io/liftbridge/server/proto"
)

const (
	stateFile                    = "liftbridge"
	serverInfoInboxTemplate      = "%s.info"
	partitionStatusInboxTemplate = "%s.status.%s"
)

// Server is the main Liftbridge object. Create it by calling New or
// RunServerWithConfig.
type Server struct {
	config             *Config
	listener           net.Listener
	nc                 *nats.Conn
	ncRaft             *nats.Conn
	ncRepl             *nats.Conn
	ncAcks             *nats.Conn
	ncPublishes        *nats.Conn
	logger             logger.Logger
	loggerOut          io.Writer
	api                *grpc.Server
	metadata           *metadataAPI
	shutdownCh         chan struct{}
	raft               atomic.Value
	leaderSub          *nats.Subscription
	recoveryStarted    bool
	latestRecoveredLog *raft.Log
	mu                 sync.RWMutex
	shutdown           bool
	running            bool
	goroutineWait      sync.WaitGroup
}

// RunServerWithConfig creates and starts a new Server with the given
// configuration. It returns an error if the Server failed to start.
func RunServerWithConfig(config *Config) (*Server, error) {
	server := New(config)
	err := server.Start()
	return server, err
}

// New creates a new Server with the given configuration. Call Start to run the
// Server.
func New(config *Config) *Server {
	// Default data path to /tmp/liftbridge/<namespace> if not set.
	if config.DataDir == "" {
		config.DataDir = filepath.Join("/tmp", "liftbridge", config.Clustering.Namespace)
	}
	logger := logger.NewLogger(config.LogLevel)
	if config.LogSilent {
		logger.SetWriter(ioutil.Discard)
	}
	s := &Server{
		config:     config,
		logger:     logger,
		shutdownCh: make(chan struct{}),
	}
	s.metadata = newMetadataAPI(s)
	return s
}

// Start the Server. This is not a blocking call. It will return an error if
// the Server cannot start properly.
func (s *Server) Start() (err error) {
	defer func() {
		if err != nil {
			s.Stop()
		}
	}()

	rand.Seed(time.Now().UnixNano())

	// Remove server's ID from the cluster peers list if present.
	if len(s.config.Clustering.RaftBootstrapPeers) > 0 {
		peers := make([]string, 0, len(s.config.Clustering.RaftBootstrapPeers))
		for _, peer := range s.config.Clustering.RaftBootstrapPeers {
			if peer != s.config.Clustering.ServerID {
				peers = append(peers, peer)
			}
		}
		s.config.Clustering.RaftBootstrapPeers = peers
	}

	// Create the data directory if it doesn't exist.
	if err := os.MkdirAll(s.config.DataDir, os.ModePerm); err != nil {
		return errors.Wrap(err, "failed to create data path directories")
	}

	// Recover and persist metadata state.
	if err := s.recoverAndPersistState(); err != nil {
		return errors.Wrap(err, "failed to recover or persist metadata state")
	}

	if err := s.createNATSConns(); err != nil {
		return errors.Wrap(err, "failed to connect to NATS")
	}

	listenAddress := s.config.GetListenAddress()
	hp := net.JoinHostPort(listenAddress.Host, strconv.Itoa(listenAddress.Port))
	l, err := net.Listen("tcp", hp)
	if err != nil {
		return errors.Wrap(err, "failed starting listener")
	}
	s.listener = l

	s.logger.Infof("Server ID:        %s", s.config.Clustering.ServerID)
	s.logger.Infof("Namespace:        %s", s.config.Clustering.Namespace)
	s.logger.Infof("Retention Policy: %s", s.config.Log.RetentionString())
	s.logger.Infof("Starting server on %s...",
		net.JoinHostPort(listenAddress.Host, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)))

	// Set a lower bound of one second for LogRollTime to avoid frequent log
	// rolls which will cause performance problems. This is mainly here because
	// LogRollTime defaults to RetentionMaxAge if it's not set explicitly, so
	// users could otherwise unknowingly cause frequent log rolls.
	if logRollTime := s.config.Log.LogRollTime; logRollTime != 0 && logRollTime < time.Second {
		s.logger.Info("Defaulting log.roll.time to 1 second to avoid frequent log rolls")
		s.config.Log.LogRollTime = time.Second
	}

	if err := s.startMetadataRaft(); err != nil {
		return errors.Wrap(err, "failed to start Raft node")
	}

	if _, err := s.ncRaft.Subscribe(s.serverInfoInbox(), s.handleServerInfoRequest); err != nil {
		return errors.Wrap(err, "failed to subscribe to server info subject")
	}

	if _, err := s.ncRaft.Subscribe(s.partitionStatusInbox(), s.handlePartitionStatusRequest); err != nil {
		return errors.Wrap(err, "failed to subscribe to partition status subject")
	}

	s.handleSignals()

	return errors.Wrap(s.startAPIServer(), "failed to start API server")
}

// Stop will attempt to gracefully shut the Server down by signaling the stop
// and waiting for all goroutines to return.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return nil
	}
	s.logger.Info("Shutting down...")

	close(s.shutdownCh)
	if s.api != nil {
		s.api.Stop()
	}

	if s.listener != nil {
		s.listener.Close()
	}

	if s.metadata != nil {
		if err := s.metadata.Reset(); err != nil {
			s.mu.Unlock()
			return err
		}
	}

	if raft := s.getRaft(); raft != nil {
		if err := raft.shutdown(); err != nil {
			s.mu.Unlock()
			return err
		}
	}

	s.closeNATSConns()
	s.running = false
	s.shutdown = true
	s.mu.Unlock()

	// Wait for goroutines to stop.
	s.goroutineWait.Wait()

	return nil
}

// IsLeader indicates if the server is currently the metadata leader or not. If
// consistency is required for an operation, it should be threaded through the
// Raft cluster since that is the single source of truth. If a server thinks
// it's leader when it's not, the operation it proposes to the Raft cluster
// will fail.
func (s *Server) IsLeader() bool {
	return atomic.LoadInt64(&(s.getRaft().leader)) == 1
}

// IsRunning indicates if the server is currently running or has been stopped.
func (s *Server) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// recoverAndPersistState recovers any existing server metadata state from disk
// to initialize the server then writes the metadata back to disk.
func (s *Server) recoverAndPersistState() error {
	// Attempt to recover state.
	file := filepath.Join(s.config.DataDir, stateFile)
	data, err := ioutil.ReadFile(file)
	if err == nil {
		// Recovered previous state.
		state := &proto.ServerState{}
		if err := state.Unmarshal(data); err == nil {
			s.config.Clustering.ServerID = state.ServerID
		}
	}

	// Persist server state.
	state := &proto.ServerState{ServerID: s.config.Clustering.ServerID}
	data, err = state.Marshal()
	if err != nil {
		panic(err)
	}
	return ioutil.WriteFile(file, data, 0666)
}

// createNATSConns creates various NATS connections used by the server,
// including connections for stream data, Raft, replication, acks, and
// publishes.
func (s *Server) createNATSConns() error {
	// NATS connection used for stream data.
	nc, err := s.createNATSConn("streams")
	if err != nil {
		return err
	}
	s.nc = nc

	// NATS connection used for Raft metadata replication.
	ncr, err := s.createNATSConn("raft")
	if err != nil {
		return err
	}
	s.ncRaft = ncr

	// NATS connection used for stream replication.
	ncRepl, err := s.createNATSConn("replication")
	if err != nil {
		return err
	}
	s.ncRepl = ncRepl

	// NATS connection used for sending acks.
	ncAcks, err := s.createNATSConn("acks")
	if err != nil {
		return err
	}
	s.ncAcks = ncAcks

	// NATS connection used for publishing messages.
	ncPublishes, err := s.createNATSConn("publishes")
	if err != nil {
		return err
	}
	s.ncPublishes = ncPublishes
	return nil
}

// closeNATSConns closes the various NATS connections used by the server,
// including connections for stream data, Raft, replication, acks, and
// publishes.
func (s *Server) closeNATSConns() {
	if s.nc != nil {
		s.nc.Close()
	}
	if s.ncRaft != nil {
		s.ncRaft.Close()
	}
	if s.ncRepl != nil {
		s.ncRepl.Close()
	}
	if s.ncAcks != nil {
		s.ncAcks.Close()
	}
	if s.ncPublishes != nil {
		s.ncPublishes.Close()
	}
}

// startAPIServer configures and starts the gRPC API server.
func (s *Server) startAPIServer() error {
	opts := []grpc.ServerOption{}

	// Setup TLS if key/cert is set.
	if s.config.TLSKey != "" && s.config.TLSCert != "" {
		creds, err := credentials.NewServerTLSFromFile(s.config.TLSCert, s.config.TLSKey)
		if err != nil {
			return errors.Wrap(err, "failed to setup TLS credentials")
		}
		opts = append(opts, grpc.Creds(creds))
	}

	api := grpc.NewServer(opts...)
	s.api = api
	client.RegisterAPIServer(api, &apiServer{s})
	s.mu.Lock()
	s.running = true
	s.mu.Unlock()
	s.startGoroutine(func() {
		err := api.Serve(s.listener)
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
		if err != nil {
			select {
			case <-s.shutdownCh:
				return
			default:
				s.logger.Fatal(err)
			}
		}
	})

	return nil
}

// createNATSConn creates a new NATS connection with the given name.
func (s *Server) createNATSConn(name string) (*nats.Conn, error) {
	var err error
	opts := s.config.NATS
	opts.Name = fmt.Sprintf("LIFT.%s.%s.%s", s.config.Clustering.Namespace, s.config.Clustering.ServerID, name)

	// Shorten the time we wait to reconnect. Don't make it too short because
	// it may exhaust the number of available FDs.
	opts.ReconnectWait = 250 * time.Millisecond

	// Try to reconnect indefinitely.
	opts.MaxReconnect = -1

	// Disable buffering in the NATS client to avoid possible duplicate
	// deliveries.
	opts.ReconnectBufSize = -1

	// Set connection handlers.
	if err = nats.ErrorHandler(s.natsErrorHandler)(&opts); err != nil {
		return nil, err
	}
	if err = nats.ReconnectHandler(s.natsReconnectedHandler)(&opts); err != nil {
		return nil, err
	}
	if err = nats.ClosedHandler(s.natsClosedHandler)(&opts); err != nil {
		return nil, err
	}
	if err = nats.DisconnectHandler(s.natsDisconnectedHandler)(&opts); err != nil {
		return nil, err
	}

	return opts.Connect()
}

// startMetadataRaft creates and starts an embedded Raft node to participate in
// the metadata cluster. This will bootstrap using the configured server
// settings and start a goroutine for automatically responding to Raft
// leadership changes.
func (s *Server) startMetadataRaft() error {
	if err := s.setupMetadataRaft(); err != nil {
		return err
	}
	node := s.getRaft()

	s.startGoroutine(func() {
		for {
			select {
			case isLeader := <-node.notifyCh:
				if isLeader {
					if err := s.leadershipAcquired(); err != nil {
						s.logger.Errorf("Error on metadata leadership acquired: %v", err)
						switch {
						case err == raft.ErrRaftShutdown:
							// Node shutdown, just return.
							return
						case err == raft.ErrLeadershipLost:
							// Node lost leadership, continue loop.
							continue
						default:
							// TODO: probably step down as leader?
							panic(err)
						}
					}
				} else {
					if err := s.leadershipLost(); err != nil {
						s.logger.Errorf("Error on metadata leadership lost: %v", err)
					}
				}
			case <-s.shutdownCh:
				return
			}
		}
	})
	return nil
}

// setRaft sets the Raft node for the server. This should only be called once
// on server start.
func (s *Server) setRaft(r *raftNode) {
	s.raft.Store(r)
}

// getRaft returns the Raft node for the server.
func (s *Server) getRaft() *raftNode {
	r := s.raft.Load()
	if r == nil {
		return nil
	}
	return r.(*raftNode)
}

// leadershipAcquired should be called when this node is elected leader.
func (s *Server) leadershipAcquired() error {
	s.logger.Infof("Server became metadata leader, performing leader promotion actions")

	// Use a barrier to ensure all preceding operations are applied to the FSM.
	if err := s.getRaft().Barrier(0).Error(); err != nil {
		return err
	}

	// Subscribe to leader NATS subject for propagated requests.
	sub, err := s.nc.Subscribe(s.getPropagateInbox(), s.handlePropagatedRequest)
	if err != nil {
		return err
	}
	s.leaderSub = sub

	atomic.StoreInt64(&(s.getRaft().leader), 1)
	return nil
}

// leadershipLost should be called when this node loses leadership.
func (s *Server) leadershipLost() error {
	s.logger.Warn("Server lost metadata leadership, performing leader stepdown actions")

	// Unsubscribe from leader NATS subject for propagated requests.
	if s.leaderSub != nil {
		if err := s.leaderSub.Unsubscribe(); err != nil {
			return err
		}
		s.leaderSub = nil
	}

	s.metadata.LostLeadership()
	atomic.StoreInt64(&(s.getRaft().leader), 0)
	return nil
}

// getPropagateInbox returns the NATS subject used for handling propagated Raft
// operations. The server subscribes to this when it is the metadata leader.
// Followers can then forward operations for the leader to apply.
func (s *Server) getPropagateInbox() string {
	return fmt.Sprintf("%s.propagate", s.baseMetadataRaftSubject())
}

// handlePropagatedRequest is a NATS handler used to process propagated Raft
// operations from followers in the Raft cluster. This is activated when the
// server becomes the metadata leader. If, for some reason, the server receives
// a forwarded operation and loses leadership at the same time, the operation
// will fail when it's proposed to the Raft cluster.
func (s *Server) handlePropagatedRequest(m *nats.Msg) {
	var (
		req  = &proto.PropagatedRequest{}
		data []byte
		err  error
	)
	if err := req.Unmarshal(m.Data); err != nil {
		s.logger.Warnf("Invalid propagated request: %v", err)
		return
	}
	switch req.Op {
	case proto.Op_CREATE_PARTITION:
		resp := &proto.PropagatedResponse{
			Op: req.Op,
		}
		if err := s.metadata.CreatePartition(context.Background(), req.CreatePartitionOp); err != nil {
			resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
		}
		data, err = resp.Marshal()
		if err != nil {
			panic(err)
		}
	case proto.Op_SHRINK_ISR:
		resp := &proto.PropagatedResponse{
			Op: req.Op,
		}
		if err := s.metadata.ShrinkISR(context.Background(), req.ShrinkISROp); err != nil {
			resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
		}
		data, err = resp.Marshal()
		if err != nil {
			panic(err)
		}
	case proto.Op_REPORT_LEADER:
		resp := &proto.PropagatedResponse{
			Op: req.Op,
		}
		if err := s.metadata.ReportLeader(context.Background(), req.ReportLeaderOp); err != nil {
			resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
		}
		data, err = resp.Marshal()
		if err != nil {
			panic(err)
		}
	case proto.Op_EXPAND_ISR:
		resp := &proto.PropagatedResponse{
			Op: req.Op,
		}
		if err := s.metadata.ExpandISR(context.Background(), req.ExpandISROp); err != nil {
			resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
		}
		data, err = resp.Marshal()
		if err != nil {
			panic(err)
		}
	default:
		s.logger.Warnf("Unknown propagated request operation: %s", req.Op)
		return
	}
	if err := m.Respond(data); err != nil {
		s.logger.Errorf("Failed to respond to propagated request: %v", err)
	}
}

func (s *Server) isShutdown() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shutdown
}

// natsDisconnectedHandler fires when the given NATS connection has been
// disconnected. This may indicate a temporary disconnect, in which case the
// client will automatically attempt to reconnect.
func (s *Server) natsDisconnectedHandler(nc *nats.Conn) {
	// If the server was shut down, do nothing since this is an expected
	// disconnect.
	if s.isShutdown() {
		return
	}
	if nc.LastError() != nil {
		s.logger.Errorf("Connection %q has been disconnected from NATS: %v",
			nc.Opts.Name, nc.LastError())
	} else {
		s.logger.Errorf("Connection %q has been disconnected from NATS", nc.Opts.Name)
	}
}

// natsReconnectedHandler fires when the given NATS connection has successfully
// reconnected.
func (s *Server) natsReconnectedHandler(nc *nats.Conn) {
	s.logger.Infof("Connection %q reconnected to NATS at %q",
		nc.Opts.Name, nc.ConnectedUrl())
}

// natsClosedHandler fires when the given NATS connection has been closed, i.e.
// permanently disconnected. At this point, the client will not attempt to
// reconnect to NATS.
func (s *Server) natsClosedHandler(nc *nats.Conn) {
	// If the server was shut down, do nothing since this is an expected close.
	if s.isShutdown() {
		return
	}
	s.logger.Debugf("Connection %q has been closed", nc.Opts.Name)
}

// natsErrorHandler fires when there is an asynchronous error on the NATS
// connection.
func (s *Server) natsErrorHandler(nc *nats.Conn, sub *nats.Subscription, err error) {
	s.logger.Errorf("Asynchronous error on connection %s, subject %s: %s",
		nc.Opts.Name, sub.Subject, err)
}

// handleServerInfoRequest is a NATS handler used to process requests for
// server information used in the metadata API.
func (s *Server) handleServerInfoRequest(m *nats.Msg) {
	req := &proto.ServerInfoRequest{}
	if err := req.Unmarshal(m.Data); err != nil {
		s.logger.Warnf("Dropping invalid server info request: %v", err)
		return
	}

	// Ignore requests from ourself.
	if req.Id == s.config.Clustering.ServerID {
		return
	}

	connectionAddress := s.config.GetConnectionAddress()
	data, err := (&proto.ServerInfoResponse{
		Id:   s.config.Clustering.ServerID,
		Host: connectionAddress.Host,
		Port: int32(connectionAddress.Port),
	}).Marshal()
	if err != nil {
		panic(err)
	}

	if err := m.Respond(data); err != nil {
		s.logger.Errorf("Failed to respond to server info request: %v", err)
	}
}

// handlePartitionStatusRequest is a NATS handler used to process requests
// querying the status of a partition. This is used as a readiness check to
// determine if a created partition has actually started.
func (s *Server) handlePartitionStatusRequest(m *nats.Msg) {
	req := &proto.PartitionStatusRequest{}
	if err := req.Unmarshal(m.Data); err != nil {
		s.logger.Warn("Dropping invalid partition status request: %v", err)
		return
	}

	partition := s.metadata.GetPartition(req.Stream, req.Partition)

	resp := &proto.PartitionStatusResponse{Exists: partition != nil}
	if partition != nil {
		resp.IsLeader = partition.IsLeader()
	}

	data, err := resp.Marshal()
	if err != nil {
		panic(err)
	}

	if err := m.Respond(data); err != nil {
		s.logger.Errorf("Failed to respond to partition status request: %v", err)
	}
}

// serverInfoInbox returns the NATS subject used for handling server
// information requests.
func (s *Server) serverInfoInbox() string {
	return fmt.Sprintf(serverInfoInboxTemplate, s.baseMetadataRaftSubject())
}

// partitionStatusInbox returns the NATS subject used for handling stream status
// requests.
func (s *Server) partitionStatusInbox() string {
	return fmt.Sprintf(partitionStatusInboxTemplate, s.baseMetadataRaftSubject(),
		s.config.Clustering.ServerID)
}

// startGoroutine starts a goroutine which is managed by the server. This adds
// the goroutine to a WaitGroup so that the server can wait for all running
// goroutines to stop on shutdown. This should be used instead of a "naked"
// goroutine.
func (s *Server) startGoroutine(f func()) {
	select {
	case <-s.shutdownCh:
		return
	default:
	}
	s.goroutineWait.Add(1)
	go func() {
		f()
		s.goroutineWait.Done()
	}()
}
