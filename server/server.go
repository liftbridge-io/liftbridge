package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/hashicorp/raft"
	client "github.com/liftbridge-io/liftbridge-api/go"
	gnatsd "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/liftbridge-io/liftbridge/server/health"
	"github.com/liftbridge-io/liftbridge/server/logger"
	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

const stateFile = "liftbridge"

const (
	streamsConnName     = "streams"
	raftConnName        = "raft"
	replicationConnName = "replication"
	acksConnName        = "acks"
	publishesConnName   = "publishes"
	activityStream      = "__activity"
	cursorsStream       = "__cursors"
)

// reservedStreams contains reserved internal stream names.
var reservedStreams = []string{activityStream, cursorsStream}

// RaftLog represents an entry into the Raft log.
type RaftLog struct {
	*raft.Log
}

// RaftLogListener is a listener for Raft logs.
type RaftLogListener interface {
	Receive(*RaftLog)
}

// authzEnforcer contains a casbin enforcer and a lock, which is used to reload permissions safely
type authzEnforcer struct {
	enforcer  *casbin.Enforcer
	authzLock sync.RWMutex
}

// Server is the main Liftbridge object. Create it by calling New or
// RunServerWithConfig.
type Server struct {
	config             *Config
	listener           net.Listener
	port               int
	embeddedNATS       *gnatsd.Server
	nc                 *nats.Conn
	ncRaft             *nats.Conn
	ncRepl             *nats.Conn
	ncAcks             *nats.Conn
	ncPublishes        *nats.Conn
	logger             logger.Logger
	grpcServer         *grpc.Server
	api                *apiServer
	metadata           *metadataAPI
	shutdownCh         chan struct{}
	raftInitialized    chan struct{}
	raft               atomic.Value
	leaderSub          *nats.Subscription
	recoveryStarted    bool
	latestRecoveredLog *raft.Log
	mu                 sync.RWMutex
	shutdown           bool
	running            bool
	goroutineWait      sync.WaitGroup
	activity           *activityManager
	cursors            *cursorManager
	raftLogListenersMu sync.RWMutex
	raftLogListeners   []RaftLogListener
	authzEnforcer      *authzEnforcer
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
		logger.Silent(true)
	}
	s := &Server{
		config:          config,
		logger:          logger,
		shutdownCh:      make(chan struct{}),
		raftInitialized: make(chan struct{}),
	}
	s.metadata = newMetadataAPI(s)
	s.activity = newActivityManager(s)
	s.cursors = newCursorManager(s)
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

	// Create the data directory if it doesn't exist.
	if err := os.MkdirAll(s.config.DataDir, os.ModePerm); err != nil {
		return errors.Wrap(err, "failed to create data path directories")
	}

	// Recover and persist metadata state.
	if err := s.recoverAndPersistState(); err != nil {
		return errors.Wrap(err, "failed to recover or persist metadata state")
	}

	s.logger.Infof("Liftbridge Version:        %s", Version)
	s.logger.Infof("Server ID:                 %s", s.config.Clustering.ServerID)
	s.logger.Infof("Namespace:                 %s", s.config.Clustering.Namespace)
	s.logger.Infof("NATS Servers:              %s", s.config.NATSServersString())
	s.logger.Infof("Default Retention Policy:  %s", s.config.Streams.RetentionString())
	s.logger.Infof("Default Partition Pausing: %s", s.config.Streams.AutoPauseString())

	// Start embedded NATS server if configured.
	if s.config.EmbeddedNATS {
		if err := s.startEmbeddedNATS(); err != nil {
			return errors.Wrap(err, "failed to start embedded NATS server")
		}
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
	s.port = l.Addr().(*net.TCPAddr).Port

	s.logger.Infof("Starting Liftbridge server on %s...",
		net.JoinHostPort(listenAddress.Host, strconv.Itoa(s.port)))

	// Set a lower bound of one second for SegmentMaxAge to avoid frequent log
	// rolls which will cause performance problems. This is mainly here because
	// SegmentMaxAge defaults to RetentionMaxAge if it's not set explicitly,
	// so users could otherwise unknowingly cause frequent log rolls.
	if logRollTime := s.config.Streams.SegmentMaxAge; logRollTime != 0 && logRollTime < time.Second {
		s.logger.Infof("Defaulting %s to 1 second to avoid frequent log rolls", configStreamsSegmentMaxAge)
		s.config.Streams.SegmentMaxAge = time.Second
	}

	raftNode, err := s.setupMetadataRaft()
	if err != nil {
		return errors.Wrap(err, "failed to start Raft node")
	}

	if _, err := s.ncRaft.Subscribe(s.getServerInfoInbox(), s.handleServerInfoRequest); err != nil {
		return errors.Wrap(err, "failed to subscribe to server info subject")
	}

	inbox := s.getPartitionStatusInbox(s.config.Clustering.ServerID)
	if _, err := s.ncRaft.Subscribe(inbox, s.handlePartitionStatusRequest); err != nil {
		return errors.Wrap(err, "failed to subscribe to partition status subject")
	}

	inbox = s.getPartitionNotificationInbox(s.config.Clustering.ServerID)
	if _, err := s.ncRepl.Subscribe(inbox, s.handlePartitionNotification); err != nil {
		return errors.Wrap(err, "failed to subscribe to partition notification subject")
	}

	s.handleSignals()

	if err := s.startAPIServer(); err != nil {
		return errors.Wrap(err, "failed to start API server")
	}

	s.startRaftLeadershipLoop(raftNode)
	return nil
}

// Stop will attempt to gracefully shut the Server down by signaling the stop
// and waiting for all goroutines to return.
func (s *Server) Stop() error {
	health.SetNotServing()
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return nil
	}

	s.logger.Info("Shutting down...")

	// Close the raftInitialized channel in case the Raft node was never
	// initialized to prevent a deadlock.
	select {
	case <-s.raftInitialized:
	default:
		close(s.raftInitialized)
	}

	close(s.shutdownCh)
	if s.grpcServer != nil {
		s.grpcServer.Stop()
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
	if s.embeddedNATS != nil {
		s.embeddedNATS.Shutdown()
	}

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
	raft := s.getRaft()
	if raft == nil {
		panic("Attempted to access Raft node but it was not initialized")
	}
	return raft.isLeader()
}

// IsRunning indicates if the server is currently running or has been stopped.
func (s *Server) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// GetListenPort returns the port the server is listening to. Returns 0 if the
// server is not listening.
func (s *Server) GetListenPort() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.port
}

// AddRaftLogListener adds a Raft log listener.
func (s *Server) AddRaftLogListener(listener RaftLogListener) {
	s.raftLogListenersMu.Lock()
	defer s.raftLogListenersMu.Unlock()
	s.raftLogListeners = append(s.raftLogListeners, listener)
}

// getConnectionAddress returns the connection address that should be used by
// the server. It uses the port the server is currently listening to if the
// connection port is 0, so that an OS-assigned port can be used as a connection
// port.
func (s *Server) getConnectionAddress() HostPort {
	s.mu.RLock()
	defer s.mu.RUnlock()

	address := s.config.GetConnectionAddress()
	if address.Port == 0 {
		address.Port = s.port
	}

	return address
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

// startEmbeddedNATS starts a NATS server embedded in this process. It returns
// once the server is ready to accept connections.
func (s *Server) startEmbeddedNATS() error {
	opts, err := gnatsd.ProcessConfigFile(s.config.EmbeddedNATSConfig)
	if err != nil {
		return err
	}
	s.embeddedNATS, err = gnatsd.NewServer(opts)
	if err != nil {
		return err
	}
	s.embeddedNATS.SetLogger(logger.NewNATSLogger(s.logger, s.config.LogNATS),
		opts.Debug, opts.Trace)
	s.logger.Infof("Starting embedded NATS server on %s",
		net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port)))
	s.startGoroutine(s.embeddedNATS.Start)
	if !s.embeddedNATS.ReadyForConnections(10 * time.Second) {
		return errors.New("unable to start embedded NATS server")
	}
	return nil
}

// createNATSConns creates various NATS connections used by the server,
// including connections for stream data, Raft, replication, acks, and
// publishes.
func (s *Server) createNATSConns() error {
	// NATS connection used for stream data.
	nc, err := s.createNATSConn(streamsConnName)
	if err != nil {
		return err
	}
	s.nc = nc

	// NATS connection used for Raft metadata replication.
	ncr, err := s.createNATSConn(raftConnName)
	if err != nil {
		return err
	}
	s.ncRaft = ncr

	// NATS connection used for stream replication.
	ncRepl, err := s.createNATSConn(replicationConnName)
	if err != nil {
		return err
	}
	s.ncRepl = ncRepl

	// NATS connection used for sending acks.
	ncAcks, err := s.createNATSConn(acksConnName)
	if err != nil {
		return err
	}
	s.ncAcks = ncAcks

	// NATS connection used for publishing messages.
	ncPublishes, err := s.createNATSConn(publishesConnName)
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
		var (
			config tls.Config
		)

		certificate, err := tls.LoadX509KeyPair(s.config.TLSCert, s.config.TLSKey)
		if err != nil {
			return errors.Wrap(err, "failed to load TLS key pair")
		}

		config.Certificates = []tls.Certificate{certificate}

		// Configure Authentication
		if s.config.TLSClientAuth {
			config.ClientAuth = tls.RequireAndVerifyClientCert

			if s.config.TLSClientAuthCA != "" {
				certPool := x509.NewCertPool()
				ca, err := ioutil.ReadFile(s.config.TLSClientAuthCA)
				if err != nil {
					return errors.Wrap(err, "failed to load TLS client ca certificate")
				}

				if ok := certPool.AppendCertsFromPEM(ca); !ok {
					return errors.Wrap(err, "failed to append TLS client certificate")
				}

				config.ClientCAs = certPool
			}
		}
		// Configure authorization
		if s.config.TLSClientAuthz {
			opts = append(opts, grpc.UnaryInterceptor(AuthzUnaryInterceptor), grpc.StreamInterceptor(AuthzStreamInterceptor))
			err = s.initAuthzEnforcer()
			if err != nil {
				return errors.Wrap(err, "failed to initialize authorization")
			}
		}

		creds := credentials.NewTLS(&config)
		opts = append(opts, grpc.Creds(creds))
	}

	grpcServer := grpc.NewServer(opts...)
	s.grpcServer = grpcServer
	s.api = &apiServer{s}
	client.RegisterAPIServer(grpcServer, s.api)

	health.Register(grpcServer)

	s.mu.Lock()
	s.running = true
	s.mu.Unlock()
	s.startGoroutine(func() {
		health.SetServing()
		err := grpcServer.Serve(s.listener)
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

// initAuthzEnforcer initializes authorization enforcer based on configuration
func (s *Server) initAuthzEnforcer() error {
	if s.config.TLSClientAuthzModel != "" && s.config.TLSClientAuthzPolicy != "" {
		s.logger.Warn("DeprecationWarning: Using file-based configuration to persist authorization is no longer supported")

		policyEnforcer, err := casbin.NewEnforcer(s.config.TLSClientAuthzModel, s.config.TLSClientAuthzPolicy)
		if err != nil {
			return err
		}
		err = policyEnforcer.LoadPolicy()

		if err != nil {
			return errors.Wrap(err, "failed to load authorization permissions")
		}
		s.authzEnforcer = &authzEnforcer{enforcer: policyEnforcer}

	} else {
		// Default ACL-with-superuser model
		authzModel, err := model.NewModelFromString(DefaultACLAuthzModel)
		if err != nil {
			return err
		}

		// Initialize casbin object without a storage file
		// as policies are stored directly on Raft
		authzStorage := fileadapter.NewAdapter("")
		policyEnforcer, err := casbin.NewEnforcer(authzModel, authzStorage)
		if err != nil {
			return err
		}
		s.authzEnforcer = &authzEnforcer{enforcer: policyEnforcer}
	}
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

	var conn *nats.Conn
	for i := 0; i < 5; i++ {
		conn, err = opts.Connect()
		if err == nil {
			return conn, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, err
}

// startRaftLeadershipLoop start a goroutine for automatically responding to
// Raft leadership changes.
func (s *Server) startRaftLeadershipLoop(node *raftNode) {
	s.startGoroutine(func() {
		for {
			select {
			case isLeader := <-node.notifyCh:
				if isLeader {
					if err := s.leadershipAcquired(node); err != nil {
						s.logger.Errorf("Error on metadata leadership acquired: %v", err)
						switch {
						case err == raft.ErrRaftShutdown:
							// Node shutdown, just return.
							return
						case err == raft.ErrLeadershipLost:
							// Node lost leadership, continue loop.
							continue
						default:
							// Step down as leader.
							s.logger.Warn("Stepping down as metadata leader")
							if future := node.LeadershipTransfer(); future.Error() != nil {
								panic(errors.Wrap(future.Error(), "error on metadata leadership step down"))
							}
							continue
						}
					}
				} else {
					if err := s.leadershipLost(node); err != nil {
						s.logger.Errorf("Error on metadata leadership lost: %v", err)
					}
				}
			case <-s.shutdownCh:
				return
			}
		}
	})
}

// setRaft sets the Raft node for the server. This should only be called once
// on server start.
func (s *Server) setRaft(r *raftNode) {
	s.raft.Store(r)
	close(s.raftInitialized)
	s.logger.Debug("Raft node initialized")
}

// getRaft returns the Raft node for the server.
func (s *Server) getRaft() *raftNode {
	<-s.raftInitialized
	r := s.raft.Load()
	if r == nil {
		s.logger.Warn("Attempted to access Raft node but it was not initialized")
		return nil
	}
	return r.(*raftNode)
}

// leadershipAcquired should be called when this node is elected leader.
func (s *Server) leadershipAcquired(raft *raftNode) error {
	s.logger.Infof("Server became metadata leader, performing leader promotion actions")

	// Use a barrier to ensure all preceding operations are applied to the FSM.
	if err := raft.Barrier(0).Error(); err != nil {
		return err
	}

	// Subscribe to leader NATS subject for propagated requests.
	sub, err := s.nc.Subscribe(s.getPropagateInbox(), s.handlePropagatedRequest)
	if err != nil {
		return err
	}
	s.leaderSub = sub

	if err := s.activity.BecomeLeader(); err != nil {
		return err
	}

	if err := s.cursors.Initialize(); err != nil {
		return err
	}

	raft.setLeader(true)
	return nil
}

// leadershipLost should be called when this node loses leadership.
func (s *Server) leadershipLost(raft *raftNode) error {
	s.logger.Warn("Server lost metadata leadership, performing leader stepdown actions")

	// Unsubscribe from leader NATS subject for propagated requests.
	if s.leaderSub != nil {
		if err := s.leaderSub.Unsubscribe(); err != nil {
			return err
		}
		s.leaderSub = nil
	}

	s.metadata.LostLeadership()

	if err := s.activity.BecomeFollower(); err != nil {
		return err
	}

	raft.setLeader(false)
	return nil
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
	var (
		msg    = "Asynchronous error on NATS connection"
		prefix = " "
	)
	if nc != nil {
		msg += fmt.Sprintf(" %s", nc.Opts.Name)
		prefix = ", "
	}
	if sub != nil {
		msg += fmt.Sprintf("%ssubject %s", prefix, sub.Subject)
	}
	msg += fmt.Sprintf(": %s", err)
	s.logger.Errorf(msg)
}

// handleServerInfoRequest is a NATS handler used to process requests for
// server information used in the metadata API.
func (s *Server) handleServerInfoRequest(m *nats.Msg) {
	req, err := proto.UnmarshalServerInfoRequest(m.Data)
	if err != nil {
		s.logger.Warnf("Dropping invalid server info request: %v", err)
		return
	}

	// Ignore requests from ourself.
	if req.Id == s.config.Clustering.ServerID {
		return
	}

	connectionAddress := s.getConnectionAddress()
	data, err := proto.MarshalServerInfoResponse(&proto.ServerInfoResponse{
		Id:   s.config.Clustering.ServerID,
		Host: connectionAddress.Host,
		Port: int32(connectionAddress.Port),
	})
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
	req, err := proto.UnmarshalPartitionStatusRequest(m.Data)
	if err != nil {
		s.logger.Warnf("Dropping invalid partition status request: %v", err)
		return
	}

	partition := s.metadata.GetPartition(req.Stream, req.Partition)

	resp := &proto.PartitionStatusResponse{Exists: partition != nil}
	if partition != nil {
		resp.IsLeader = partition.IsLeader()
	}

	data, err := proto.MarshalPartitionStatusResponse(resp)
	if err != nil {
		panic(err)
	}

	if err := m.Respond(data); err != nil {
		s.logger.Errorf("Failed to respond to partition status request: %v", err)
	}
}

// handlePartitionNotification is a NATS handler used to process notifications
// from a leader that new data is available on a partition for the follower to
// replicate if the follower is idle.
//
// When a follower reaches the end of the log, it starts to sleep in between
// replication requests to avoid overloading the leader. However, this causes
// added commit latency when new messages are published to the log since the
// follower is idle. As a result, the leader will note when a follower is
// caught up and send a notification in order to wake an idle follower back up
// when new data is written to the log.
func (s *Server) handlePartitionNotification(m *nats.Msg) {
	req, err := proto.UnmarshalPartitionNotification(m.Data)
	if err != nil {
		s.logger.Warnf("Dropping invalid partition notification: %v", err)
		return
	}

	partition := s.metadata.GetPartition(req.Stream, req.Partition)
	if partition == nil {
		s.logger.Warnf("Dropping invalid partition notification: no partition %d for stream %s",
			req.Partition, req.Stream)
		return
	}

	// Wake the follower up.
	partition.Notify()
}

// getServerInfoInbox returns the NATS subject used for handling server
// information requests.
func (s *Server) getServerInfoInbox() string {
	return fmt.Sprintf("%s.info", s.baseMetadataRaftSubject())
}

// getPartitionStatusInbox returns the NATS subject used for handling stream
// status requests.
func (s *Server) getPartitionStatusInbox(id string) string {
	return fmt.Sprintf("%s.status.%s", s.baseMetadataRaftSubject(), id)
}

// getMetadataReplyInbox returns a random NATS subject to use for metadata
// responses scoped to the cluster namespace.
func (s *Server) getMetadataReplyInbox() string {
	return fmt.Sprintf("%s.fetch.%s", s.baseMetadataRaftSubject(), nuid.Next())
}

// getPartitionNotificationInbox returns the NATS subject used for leaders to
// indicate new data is available on a partition for a follower to replicate if
// the follower is idle.
func (s *Server) getPartitionNotificationInbox(id string) string {
	return fmt.Sprintf("%s.notify.%s", s.config.Clustering.Namespace, id)
}

// getAckInbox returns a random NATS subject to use for publish acks scoped to
// the cluster namespace.
func (s *Server) getAckInbox() string {
	return fmt.Sprintf("%s.ack.%s", s.config.Clustering.Namespace, nuid.Next())
}

// getActivityStreamSubject returns the NATS subject used for publishing
// activity stream events.
func (s *Server) getActivityStreamSubject() string {
	return fmt.Sprintf("%s.activity", s.config.Clustering.Namespace)
}

// getCursorStreamSubject returns the NATS subject used for storing consumer
// partition cursors.
func (s *Server) getCursorStreamSubject() string {
	return fmt.Sprintf("%s.cursors", s.config.Clustering.Namespace)
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

// startGoroutineWG starts a goroutine which is managed by the server and calls
// Done() on the provided WaitGroup upon completion. This adds the goroutine to
// a WaitGroup so that the server can wait for all running goroutines to stop
// on shutdown. This should be used instead of a "naked" goroutine.
func (s *Server) startGoroutineWG(f func(), wg sync.WaitGroup) {
	select {
	case <-s.shutdownCh:
		return
	default:
	}
	s.goroutineWait.Add(1)
	wg.Add(1)
	go func() {
		f()
		wg.Done()
		s.goroutineWait.Done()
	}()
}

// startGoroutineWithArgs starts a goroutine which is managed by the server and
// is passed the provided arguments. This adds the goroutine to a WaitGroup so
// that the server can wait for all running goroutines to stop on shutdown.
// This should be used instead of a "naked" goroutine.
func (s *Server) startGoroutineWithArgs(f func(...interface{}), args ...interface{}) {
	select {
	case <-s.shutdownCh:
		return
	default:
	}
	s.goroutineWait.Add(1)
	go func() {
		f(args...)
		s.goroutineWait.Done()
	}()
}

// startGoroutineWithArgsWG starts a goroutine which is managed by the server
// and is passed the provided arguments and calls Done() on the provided
// WaitGroup upon completion. This adds the goroutine to a WaitGroup so that
// the server can wait for all running goroutines to stop on shutdown. This
// should be used instead of a "naked" goroutine.
func (s *Server) startGoroutineWithArgsWG(f func(...interface{}), wg sync.WaitGroup, args ...interface{}) {
	select {
	case <-s.shutdownCh:
		return
	default:
	}
	s.goroutineWait.Add(1)
	wg.Add(1)
	go func() {
		f(args...)
		wg.Done()
		s.goroutineWait.Done()
	}()
}
