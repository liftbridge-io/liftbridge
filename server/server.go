package server

import (
	"net"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tylertreat/go-jetbridge/proto"
	"google.golang.org/grpc"

	"github.com/tylertreat/jetbridge/server/metadata"
)

const defaultNamespace = "jetbridge-default"

type Server struct {
	config   Config
	listener net.Listener
	nats     *nats.Conn
	logger   *log.Logger
	api      *grpc.Server
	metadata metadata.MetadataStore

	// Controller replication.
	raft        *raft.Raft
	raftLogging bool
}

type Config struct {
	Logger           *log.Logger
	NATSOpts         nats.Options
	Addr             string
	NodeID           string
	ClusterNamespace string
	RaftPath         string
	RaftSnapshots    int
	RaftCacheSize    int
}

func New(config Config) *Server {
	if config.NodeID == "" {
		config.NodeID = nuid.Next()
	}
	if config.ClusterNamespace == "" {
		config.ClusterNamespace = defaultNamespace
	}
	// Default Raft log path to ./<namespace>/<node-id> if not set.
	if config.RaftPath == "" {
		config.RaftPath = filepath.Join(config.ClusterNamespace, config.NodeID)
	}
	return &Server{
		config:   config,
		logger:   config.Logger,
		metadata: metadata.NewMetadataStore(),
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

	if err := s.setupRaft("metadata"); err != nil {
		l.Close()
		nc.Close()
		return err
	}

	api := grpc.NewServer()
	s.api = api
	proto.RegisterAPIServer(api, &apiServer{logger: s.logger})
	s.logger.Info("Starting server...")
	return api.Serve(s.listener)
}

func (s *Server) Stop() {
	s.api.Stop()
}
