package server

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	client "github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"github.com/natefinch/lumberjack"

	"github.com/liftbridge-io/liftbridge/server/logger"
	"github.com/liftbridge-io/liftbridge/server/proto"
)

const (
	stateFile                 = "liftbridge"
	serverInfoInboxTemplate   = "%s.info"
	streamStatusInboxTemplate = "%s.status.%s"
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
	logger             logger.Logger
	api                *grpc.Server
	metadata           *metadataAPI
	shutdownCh         chan struct{}
	raft               *raftNode
	leaderSub          *nats.Subscription
	startedRecovery    bool
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
	logger := log.New()
	logger.SetLevel(log.Level(config.LogLevel))
	logFormatter := &log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	}
	logger.Formatter = logFormatter
	logger.Out = &lumberjack.Logger{
		Filename:   config.LogFilename,
		MaxSize:    config.LogMaxSize,
		MaxBackups: config.LogMaxBackups,
		MaxAge:     config.LogMaxAge,
		LocalTime:  config.LogLocalTime,
		Compress:   config.LogCompress,
	}
	if config.NoLog {
		logger.Out = ioutil.Discard
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
func (s *Server) Start() error {
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

	if err := os.MkdirAll(s.config.DataDir, os.ModePerm); err != nil {
		return errors.Wrap(err, "failed to create data path directories")
	}

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
	if err := ioutil.WriteFile(file, data, 0666); err != nil {
		return errors.Wrap(err, "failed to persist server state")
	}

	hp := net.JoinHostPort(s.config.Host, strconv.Itoa(s.config.Port))
	l, err := net.Listen("tcp", hp)
	if err != nil {
		return errors.Wrap(err, "failed starting listener")
	}
	s.listener = l

	// NATS connection used for stream data.
	nc, err := s.createNATSConn("streams")
	if err != nil {
		s.Stop()
		return errors.Wrap(err, "failed to connect to NATS")
	}
	s.nc = nc

	// NATS connection used for Raft metadata replication.
	ncr, err := s.createNATSConn("raft")
	if err != nil {
		s.Stop()
		return errors.Wrap(err, "failed to connect to NATS")
	}
	s.ncRaft = ncr

	// NATS connection used for stream replication.
	ncRepl, err := s.createNATSConn("replication")
	if err != nil {
		s.Stop()
		return errors.Wrap(err, "failed to connect to NATS")
	}
	s.ncRepl = ncRepl

	// NATS connection used for sending acks.
	ncAcks, err := s.createNATSConn("acks")
	if err != nil {
		s.Stop()
		return errors.Wrap(err, "failed to connect to NATS")
	}
	s.ncAcks = ncAcks

	s.logger.Infof("Server ID: %s", s.config.Clustering.ServerID)
	s.logger.Infof("Namespace: %s", s.config.Clustering.Namespace)
	s.logger.Infof("Starting server on %s...",
		net.JoinHostPort(s.config.Host, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)))

	if err := s.startMetadataRaft(); err != nil {
		s.Stop()
		return errors.Wrap(err, "failed to start Raft node")
	}

	if _, err := s.ncRaft.Subscribe(s.serverInfoInbox(), s.handleServerInfoRequest); err != nil {
		s.Stop()
		return errors.Wrap(err, "failed to subscribe to server info subject")
	}

	if _, err := s.ncRaft.Subscribe(s.streamStatusInbox(), s.handleStreamStatusRequest); err != nil {
		s.Stop()
		return errors.Wrap(err, "failed to subscribe to stream status subject")
	}

	s.handleSignals()

	api := grpc.NewServer()
	s.api = api
	client.RegisterAPIServer(api, &apiServer{s})
	s.mu.Lock()
	s.running = true
	s.mu.Unlock()
	s.startGoroutine(func() {
		err = api.Serve(s.listener)
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
	return err
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

	if s.raft != nil {
		if err := s.raft.shutdown(); err != nil {
			s.mu.Unlock()
			return err
		}
	}

	if s.nc != nil {
		s.nc.Close()
	}
	if s.ncRaft != nil {
		s.ncRaft.Close()
	}
	if s.ncRepl != nil {
		s.ncRepl.Close()
	}

	s.running = false
	s.shutdown = true
	s.mu.Unlock()

	// Wait for goroutines to stop.
	s.goroutineWait.Wait()

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

// finishedRecovery should be called when the FSM has finished replaying any
// unapplied log entries. This will start any streams recovered during the
// replay.
func (s *Server) finishedRecovery() (int, error) {
	count := 0
	for _, stream := range s.metadata.GetStreams() {
		recovered, err := stream.StartRecovered()
		if err != nil {
			return 0, err
		}
		if recovered {
			count++
		}
	}
	return count, nil
}

// startMetadataRaft creates and starts an embedded Raft node to participate in
// the metadata cluster. This will bootstrap using the configured server
// settings and start a goroutine for automatically responding to Raft
// leadership changes.
func (s *Server) startMetadataRaft() error {
	if err := s.setupMetadataRaft(); err != nil {
		return err
	}
	node := s.raft

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

// leadershipAcquired should be called when this node is elected leader.
func (s *Server) leadershipAcquired() error {
	s.logger.Infof("Server became metadata leader, performing leader promotion actions")

	// Use a barrier to ensure all preceding operations are applied to the FSM.
	if err := s.raft.Barrier(0).Error(); err != nil {
		return err
	}

	// Subscribe to leader NATS subject for propagated requests.
	sub, err := s.nc.Subscribe(s.getPropagateInbox(), s.handlePropagatedRequest)
	if err != nil {
		return err
	}
	s.leaderSub = sub

	atomic.StoreInt64(&s.raft.leader, 1)
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
	atomic.StoreInt64(&s.raft.leader, 0)
	return nil
}

// isLeader indicates if the server is currently the metadata leader or not. If
// consistency is required for an operation, it should be threaded through the
// Raft cluster since that is the single source of truth. If a server thinks
// it's leader when it's not, the operation it proposes to the Raft cluster
// will fail.
func (s *Server) isLeader() bool {
	return atomic.LoadInt64(&s.raft.leader) == 1
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
	if m.Reply == "" {
		s.logger.Warn("Invalid propagated request: no reply inbox")
		return
	}
	req := &proto.PropagatedRequest{}
	if err := req.Unmarshal(m.Data); err != nil {
		s.logger.Warnf("Invalid propagated request: %v", err)
		return
	}
	switch req.Op {
	case proto.Op_CREATE_STREAM:
		resp := &proto.PropagatedResponse{
			Op:               req.Op,
			CreateStreamResp: &client.CreateStreamResponse{},
		}
		if err := s.metadata.CreateStream(context.Background(), req.CreateStreamOp); err != nil {
			resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
		}
		data, err := resp.Marshal()
		if err != nil {
			panic(err)
		}
		s.nc.Publish(m.Reply, data)
	case proto.Op_SHRINK_ISR:
		resp := &proto.PropagatedResponse{
			Op: req.Op,
		}
		if err := s.metadata.ShrinkISR(context.Background(), req.ShrinkISROp); err != nil {
			resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
		}
		data, err := resp.Marshal()
		if err != nil {
			panic(err)
		}
		s.nc.Publish(m.Reply, data)
	case proto.Op_REPORT_LEADER:
		resp := &proto.PropagatedResponse{
			Op: req.Op,
		}
		if err := s.metadata.ReportLeader(context.Background(), req.ReportLeaderOp); err != nil {
			resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
		}
		data, err := resp.Marshal()
		if err != nil {
			panic(err)
		}
		s.nc.Publish(m.Reply, data)
	case proto.Op_EXPAND_ISR:
		resp := &proto.PropagatedResponse{
			Op: req.Op,
		}
		if err := s.metadata.ExpandISR(context.Background(), req.ExpandISROp); err != nil {
			resp.Error = &proto.Error{Code: uint32(err.Code()), Msg: err.Message()}
		}
		data, err := resp.Marshal()
		if err != nil {
			panic(err)
		}
		s.nc.Publish(m.Reply, data)
	default:
		s.logger.Warnf("Unknown propagated request operation: %s", req.Op)
	}
}

func (s *Server) isRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
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
	if m.Reply == "" {
		s.logger.Warn("Dropping server info request with no reply inbox")
		return
	}
	req := &proto.ServerInfoRequest{}
	if err := req.Unmarshal(m.Data); err != nil {
		s.logger.Warn("Dropping invalid server info request: %v", err)
		return
	}

	// Ignore requests from ourself.
	if req.Id == s.config.Clustering.ServerID {
		return
	}

	data, err := (&proto.ServerInfoResponse{
		Id:   s.config.Clustering.ServerID,
		Host: s.config.Host,
		Port: int32(s.config.Port),
	}).Marshal()
	if err != nil {
		panic(err)
	}

	s.ncRaft.Publish(m.Reply, data)
}

// handleStreamStatusRequest is a NATS handler used to process requests
// querying the status of a stream. This is used as a readiness check to
// determine if a created stream has actually started.
func (s *Server) handleStreamStatusRequest(m *nats.Msg) {
	if m.Reply == "" {
		s.logger.Warn("Dropping stream status request with no reply inbox")
		return
	}
	req := &proto.StreamStatusRequest{}
	if err := req.Unmarshal(m.Data); err != nil {
		s.logger.Warn("Dropping invalid stream status request: %v", err)
		return
	}

	stream := s.metadata.GetStream(req.Subject, req.Name)

	resp := &proto.StreamStatusResponse{Exists: stream != nil}
	if stream != nil {
		resp.IsLeader = stream.IsLeader()
	}

	data, err := resp.Marshal()
	if err != nil {
		panic(err)
	}

	s.ncRaft.Publish(m.Reply, data)
}

// serverInfoInbox returns the NATS subject used for handling server
// information requests.
func (s *Server) serverInfoInbox() string {
	return fmt.Sprintf(serverInfoInboxTemplate, s.baseMetadataRaftSubject())
}

// streamStatusInbox returns the NATS subject used for handling stream status
// requests.
func (s *Server) streamStatusInbox() string {
	return fmt.Sprintf(streamStatusInboxTemplate, s.baseMetadataRaftSubject(),
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
