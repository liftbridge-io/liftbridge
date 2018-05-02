package server

import (
	"net"
	"path/filepath"
	"sync/atomic"

	"github.com/hashicorp/raft"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tylertreat/go-jetbridge/proto"
	"google.golang.org/grpc"
)

const defaultNamespace = "jetbridge-default"

type Server struct {
	config     Config
	listener   net.Listener
	nats       *nats.Conn
	raftNats   *nats.Conn
	logger     *log.Logger
	api        *grpc.Server
	metadata   MetadataStore
	shutdownCh chan struct{}
	raft       *raftNode
}

type Config struct {
	Logger   *log.Logger
	NATSOpts nats.Options
	Addr     string
	Log      struct {
		RetentionBytes  int64
		MaxSegmentBytes int64
	}
	Clustering struct {
		NodeID         string
		Namespace      string
		RaftPath       string
		RaftSnapshots  int
		RaftCacheSize  int
		Bootstrap      bool
		BootstrapPeers []string
		RaftLogging    bool
	}
}

func New(config Config) *Server {
	if config.Clustering.NodeID == "" {
		config.Clustering.NodeID = nuid.Next()
	}
	if config.Clustering.Namespace == "" {
		config.Clustering.Namespace = defaultNamespace
	}
	// Default Raft log path to ./<namespace>/<node-id> if not set.
	if config.Clustering.RaftPath == "" {
		config.Clustering.RaftPath = filepath.Join(
			config.Clustering.Namespace, config.Clustering.NodeID)
	}
	return &Server{
		config:   config,
		logger:   config.Logger,
		metadata: newMetadataStore(),
	}
}

func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return errors.Wrap(err, "failed starting listener")
	}
	s.listener = l
	nc, err := s.config.NATSOpts.Connect()
	if err != nil {
		l.Close()
		return errors.Wrap(err, "failed to connect to NATS")
	}
	s.nats = nc

	// Use a separate NATS connection for Raft.
	ncr, err := s.config.NATSOpts.Connect()
	if err != nil {
		l.Close()
		return errors.Wrap(err, "failed to connect to NATS")
	}
	s.raftNats = ncr

	if err := s.startMetadataRaft(); err != nil {
		l.Close()
		nc.Close()
		return err
	}

	api := grpc.NewServer()
	s.api = api
	proto.RegisterAPIServer(api, &apiServer{s})
	s.logger.Info("Starting server...")
	return api.Serve(s.listener)
}

func (s *Server) startMetadataRaft() error {
	if err := s.setupMetadataRaft(); err != nil {
		return err
	}
	node := s.raft

	leaderWait := make(chan struct{}, 1)
	leaderReady := func() {
		select {
		case leaderWait <- struct{}{}:
		default:
		}
	}
	if node.State() != raft.Leader {
		leaderReady()
	}

	go func() {
		for {
			select {
			case isLeader := <-node.notifyCh:
				if isLeader {
					err := s.leadershipAcquired()
					leaderReady()
					if err != nil {
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
					s.leadershipLost()
				}
			case <-s.shutdownCh:
				// Signal channel here to handle edge case where we might
				// otherwise block forever on the channel when shutdown.
				leaderReady()
				return
			}
		}
	}()

	<-leaderWait
	return nil
}

// leadershipAcquired should be called when this node is elected leader.
func (s *Server) leadershipAcquired() error {
	s.logger.Infof("Server became metadata leader, performing leader promotion actions")
	defer s.logger.Infof("Finished metadata leader promotion actions")

	atomic.StoreInt64(&s.raft.leader, 1)
	return nil
}

// leadershipLost should be called when this node loses leadership.
func (s *Server) leadershipLost() {
	s.logger.Infof("Sserver lost metadata leadership, performing leader stepdown actions")
	defer s.logger.Infof("Finished metadata leader stepdown actions")

	atomic.StoreInt64(&s.raft.leader, 0)
}

func (s *Server) isLeader() bool {
	return atomic.LoadInt64(&s.raft.leader) == 1
}

func (s *Server) Stop() error {
	close(s.shutdownCh)
	s.api.Stop()

	if err := s.metadata.Reset(); err != nil {
		return err
	}

	if err := s.raft.shutdown(); err != nil {
		return err
	}
	s.nats.Close()
	s.raftNats.Close()

	return nil
}
