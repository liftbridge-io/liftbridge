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
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	client "github.com/tylertreat/go-liftbridge/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/tylertreat/liftbridge/server/logger"
	"github.com/tylertreat/liftbridge/server/proto"
)

const stateFile = "liftbridge"

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

func New(config *Config) *Server {
	// Default data path to /tmp/liftbridge/<namespace> if not set.
	if config.DataPath == "" {
		config.DataPath = filepath.Join("/tmp", "liftbridge", config.Clustering.Namespace)
	}
	logger := log.New()
	logger.SetLevel(log.Level(config.LogLevel))
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

func (s *Server) Start() error {
	if err := os.MkdirAll(s.config.DataPath, os.ModePerm); err != nil {
		return errors.Wrap(err, "failed to create data path directories")
	}

	// Attempt to recover state.
	file := filepath.Join(s.config.DataPath, stateFile)
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

	if _, err := s.ncRaft.Subscribe(s.advertiseInbox(), s.handleAdvertiseQuery); err != nil {
		s.Stop()
		return errors.Wrap(err, "failed to subscribe to advertise subject")
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

func (s *Server) createNATSConn(name string) (*nats.Conn, error) {
	var err error
	opts := s.config.NATS
	opts.Name = fmt.Sprintf("LB-%s-%s", s.config.Clustering.ServerID, name)
	opts.ReconnectWait = 250 * time.Millisecond
	opts.MaxReconnect = -1
	opts.ReconnectBufSize = -1

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

func (s *Server) isLeader() bool {
	return atomic.LoadInt64(&s.raft.leader) == 1
}

func (s *Server) getPropagateInbox() string {
	return fmt.Sprintf("%s.propagate", s.baseMetadataRaftSubject())
}

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

func (s *Server) natsDisconnectedHandler(nc *nats.Conn) {
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

func (s *Server) natsReconnectedHandler(nc *nats.Conn) {
	s.logger.Infof("Connection %q reconnected to NATS at %q",
		nc.Opts.Name, nc.ConnectedUrl())
}

func (s *Server) natsClosedHandler(nc *nats.Conn) {
	if s.isShutdown() {
		return
	}
	s.logger.Debugf("Connection %q has been closed", nc.Opts.Name)
}

func (s *Server) natsErrorHandler(nc *nats.Conn, sub *nats.Subscription, err error) {
	s.logger.Errorf("Asynchronous error on connection %s, subject %s: %s",
		nc.Opts.Name, sub.Subject, err)
}

func (s *Server) handleAdvertiseQuery(m *nats.Msg) {
	if m.Reply == "" {
		s.logger.Warn("Dropping advertise query request with no reply inbox")
		return
	}
	req := &proto.AdvertiseQueryRequest{}
	if err := req.Unmarshal(m.Data); err != nil {
		s.logger.Warn("Dropping invalid advertise query request: %v", err)
		return
	}

	// Ignore requests from ourself.
	if req.Id == s.config.Clustering.ServerID {
		return
	}

	data, err := (&proto.AdvertiseQueryResponse{
		Id:   s.config.Clustering.ServerID,
		Host: s.config.Host,
		Port: int32(s.config.Port),
	}).Marshal()
	if err != nil {
		panic(err)
	}

	s.ncRaft.Publish(m.Reply, data)
}

func (s *Server) advertiseInbox() string {
	return fmt.Sprintf("%s.advertise", s.baseMetadataRaftSubject())
}

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
