package server

import (
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	client "github.com/tylertreat/go-jetbridge/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tylertreat/jetbridge/server/proto"
)

const defaultPropagateTimeout = 5 * time.Second

type subjectStreams map[string]*stream

type metadataAPI struct {
	*Server
	streams map[string]subjectStreams
	mu      sync.RWMutex
}

func newMetadataAPI(s *Server) *metadataAPI {
	return &metadataAPI{
		Server:  s,
		streams: make(map[string]subjectStreams),
	}
}

func (m *metadataAPI) CreateStream(ctx context.Context, req *client.CreateStreamRequest) *status.Status {
	// Forward the request if we're not the leader.
	if !m.isLeader() {
		return m.propagateCreateStream(ctx, req)
	}

	// Select replicationFactor nodes to participate in the stream.
	// TODO: Currently this selection is random but could be made more
	// intelligent, e.g. selecting based on current load.
	replicas, st := m.getStreamReplicas(req.ReplicationFactor)
	if st != nil {
		return st
	}

	// Select a leader at random.
	leader := replicas[rand.Intn(len(replicas))]

	// Replicate stream create through Raft.
	op := &proto.RaftLog{
		Op: proto.Op_CREATE_STREAM,
		CreateStreamOp: &proto.CreateStreamOp{
			Stream: &proto.Stream{
				Subject:           req.Subject,
				Name:              req.Name,
				ConsumerGroup:     req.ConsumerGroup,
				ReplicationFactor: req.ReplicationFactor,
				Replicas:          replicas,
				Leader:            leader,
				Isr:               replicas,
			},
		},
	}

	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}

	// Wait on result of replication.
	if err := m.raft.Apply(data, raftApplyTimeout).Error(); err != nil {
		return status.New(codes.Internal, "Failed to replicate stream")
	}

	return nil
}

func (m *metadataAPI) ShrinkISR(ctx context.Context, req *proto.ShrinkISROp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.isLeader() {
		return m.propagateShrinkISR(ctx, req)
	}

	// Replicate ISR shrink through Raft.
	op := &proto.RaftLog{
		Op:          proto.Op_SHRINK_ISR,
		ShrinkISROp: req,
	}

	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}

	// Wait on result of replication.
	if err := m.raft.Apply(data, raftApplyTimeout).Error(); err != nil {
		return status.New(codes.Internal, "Failed to shrink ISR")
	}

	return nil
}

func (m *metadataAPI) AddStream(stream *stream) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	streams := m.streams[stream.Subject]
	if streams == nil {
		streams = make(subjectStreams)
		m.streams[stream.Subject] = streams
	}
	if _, ok := streams[stream.Name]; ok {
		// Stream for subject with name already exists.
		return nil
	}
	streams[stream.Name] = stream
	return nil
}

func (m *metadataAPI) GetStreams() []*stream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]*stream, 0, len(m.streams))
	for _, streams := range m.streams {
		for _, stream := range streams {
			ret = append(ret, stream)
		}
	}
	return ret
}

func (m *metadataAPI) GetStream(subject, name string) *stream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	streams := m.streams[subject]
	if streams == nil {
		return nil
	}
	stream := streams[name]
	if stream == nil {
		return nil
	}
	return stream
}

func (m *metadataAPI) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, streams := range m.streams {
		for _, stream := range streams {
			if err := stream.close(); err != nil {
				return err
			}
		}
	}
	m.streams = make(map[string]subjectStreams)
	return nil
}

func (m *metadataAPI) getStreamReplicas(replicationFactor int32) ([]string, *status.Status) {
	ids, err := m.getClusterServerIDs()
	if err != nil {
		return nil, status.New(codes.Internal, err.Error())
	}
	if replicationFactor <= 0 {
		return nil, status.Newf(codes.InvalidArgument, "Invalid replicationFactor %d", replicationFactor)
	}
	if replicationFactor > int32(len(ids)) {
		return nil, status.Newf(codes.InvalidArgument, "Invalid replicationFactor %d, cluster size %d",
			replicationFactor, len(ids))
	}
	var (
		indexes  = rand.Perm(len(ids))
		replicas = make([]string, replicationFactor)
	)
	for i := int32(0); i < replicationFactor; i++ {
		replicas[i] = ids[indexes[i]]
	}
	return replicas, nil
}

func (m *metadataAPI) getClusterServerIDs() ([]string, error) {
	future := m.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, errors.Wrap(err, "failed to get cluster configuration")
	}
	var (
		servers = future.Configuration().Servers
		ids     = make([]string, len(servers))
	)
	for i, server := range servers {
		ids[i] = string(server.ID)
	}
	return ids, nil
}

func (m *metadataAPI) propagateCreateStream(ctx context.Context, req *client.CreateStreamRequest) *status.Status {
	m.logger.Info("Server is not metadata leader, propagating CreateStream request")
	propagate, err := (&proto.PropagatedRequest{
		Op:             proto.Op_CREATE_STREAM,
		CreateStreamOp: req,
	}).Marshal()
	if err != nil {
		panic(err)
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultPropagateTimeout)
		defer cancel()
	}

	resp, err := m.nc.RequestWithContext(ctx, m.getPropagateInbox(), propagate)
	if err != nil {
		return status.New(codes.Internal, err.Error())
	}

	r := &proto.PropagatedResponse{}
	if err := r.Unmarshal(resp.Data); err != nil {
		m.logger.Errorf("Invalid response for propagated request: %v", err)
	}
	if r.Error != nil {
		return status.New(codes.Code(r.Error.Code), r.Error.Msg)
	}

	return nil
}

func (m *metadataAPI) propagateShrinkISR(ctx context.Context, req *proto.ShrinkISROp) *status.Status {
	m.logger.Info("Server is not metadata leader, propagating ShrinkISR request")
	propagate, err := (&proto.PropagatedRequest{
		Op:          proto.Op_SHRINK_ISR,
		ShrinkISROp: req,
	}).Marshal()
	if err != nil {
		panic(err)
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultPropagateTimeout)
		defer cancel()
	}

	resp, err := m.nc.RequestWithContext(ctx, m.getPropagateInbox(), propagate)
	if err != nil {
		return status.New(codes.Internal, err.Error())
	}

	r := &proto.PropagatedResponse{}
	if err := r.Unmarshal(resp.Data); err != nil {
		m.logger.Errorf("Invalid response for propagated request: %v", err)
	}
	if r.Error != nil {
		return status.New(codes.Code(r.Error.Code), r.Error.Msg)
	}

	return nil
}
