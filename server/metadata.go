package server

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	client "github.com/tylertreat/go-liftbridge/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tylertreat/liftbridge/server/proto"
)

const defaultPropagateTimeout = 5 * time.Second

var ErrStreamExists = errors.New("stream already exists")

type subjectStreams map[string]*stream

type leaderReported struct {
	mu              sync.Mutex
	stream          *stream
	timer           *time.Timer
	witnessReplicas map[string]struct{}
	api             *metadataAPI
}

func (l *leaderReported) addWitness(replica string) *status.Status {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.witnessReplicas[replica] = struct{}{}

	var (
		// Subtract 1 to exclude leader.
		isrSize      = l.stream.ISRSize() - 1
		leaderFailed = len(l.witnessReplicas) >= isrSize/2+1
	)

	if leaderFailed {
		if l.timer != nil {
			l.timer.Stop()
		}
		return l.api.electNewStreamLeader(l.stream)
	}

	if l.timer != nil {
		l.timer.Reset(l.api.config.Clustering.ReplicaMaxLeaderTimeout)
	} else {
		l.timer = time.AfterFunc(
			l.api.config.Clustering.ReplicaMaxLeaderTimeout, func() {
				l.api.mu.Lock()
				delete(l.api.leaderReports, l.stream)
				l.api.mu.Unlock()
			})
	}
	return nil
}

func (l *leaderReported) cancel() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.timer != nil {
		l.timer.Stop()
	}
}

type metadataAPI struct {
	*Server
	streams       map[string]subjectStreams
	mu            sync.RWMutex
	leaderReports map[*stream]*leaderReported
}

func newMetadataAPI(s *Server) *metadataAPI {
	return &metadataAPI{
		Server:        s,
		streams:       make(map[string]subjectStreams),
		leaderReports: make(map[*stream]*leaderReported),
	}
}

func (m *metadataAPI) FetchMetadata(ctx context.Context, req *client.FetchMetadataRequest) (
	*client.FetchMetadataResponse, *status.Status) {

	resp := m.createMetadataResponse(req.Streams)

	servers, err := m.getClusterServerIDs()
	if err != nil {
		return nil, status.New(codes.Internal, err.Error())
	}
	numPeers := len(servers) - 1
	if numPeers == 0 {
		return resp, nil
	}

	// Query broker info from peers.
	// TODO: cache this?
	if err := m.fetchBrokerInfo(ctx, resp, numPeers); err != nil {
		return nil, err
	}

	return resp, nil
}

func (m *metadataAPI) fetchBrokerInfo(ctx context.Context, resp *client.FetchMetadataResponse,
	numPeers int) *status.Status {

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultPropagateTimeout)
		defer cancel()
	}
	inbox := nats.NewInbox()
	sub, err := m.ncRaft.SubscribeSync(inbox)
	if err != nil {
		return status.New(codes.Internal, err.Error())
	}
	defer sub.Unsubscribe()

	queryReq, err := (&proto.AdvertiseQueryRequest{
		Id: m.config.Clustering.ServerID,
	}).Marshal()
	if err != nil {
		panic(err)
	}
	m.ncRaft.PublishRequest(m.advertiseInbox(), inbox, queryReq)

	for i := 0; i < numPeers; i++ {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			break
		}
		queryResp := &proto.AdvertiseQueryResponse{}
		if err := queryResp.Unmarshal(msg.Data); err != nil {
			m.logger.Warnf("Received invalid advertise query response: %v", err)
			continue
		}
		resp.Brokers = append(resp.Brokers, &client.Broker{
			Id:   queryResp.Id,
			Host: queryResp.Host,
			Port: queryResp.Port,
		})
	}

	return nil
}

func (m *metadataAPI) createMetadataResponse(streams []*client.StreamDescriptor) *client.FetchMetadataResponse {
	brokers := []*client.Broker{&client.Broker{
		Id:   m.config.Clustering.ServerID,
		Host: m.config.Host,
		Port: int32(m.config.Port),
	}}

	// If no descriptors were provided, fetch metadata for all streams.
	if len(streams) == 0 {
		for _, stream := range m.GetStreams() {
			streams = append(streams, &client.StreamDescriptor{
				Subject: stream.Subject,
				Name:    stream.Name,
			})
		}
	}

	metadata := make([]*client.StreamMetadata, len(streams))

	for i, descriptor := range streams {
		stream := m.GetStream(descriptor.Subject, descriptor.Name)
		if stream == nil {
			// Stream does not exist.
			metadata[i] = &client.StreamMetadata{
				Stream: descriptor,
				Error:  client.StreamMetadata_UNKNOWN_STREAM,
			}
		} else {
			leader, _ := stream.GetLeader()
			metadata[i] = &client.StreamMetadata{
				Stream:   descriptor,
				Leader:   leader,
				Replicas: stream.GetReplicas(),
				Isr:      stream.GetISR(),
			}
		}
	}

	return &client.FetchMetadataResponse{Brokers: brokers, Metadata: metadata}
}

func (m *metadataAPI) CreateStream(ctx context.Context, req *client.CreateStreamRequest) *status.Status {
	// Forward the request if we're not the leader.
	if !m.isLeader() {
		return m.propagateCreateStream(ctx, req)
	}

	// Select replicationFactor nodes to participate in the stream.
	replicas, st := m.getStreamReplicas(req.ReplicationFactor)
	if st != nil {
		return st
	}

	// Select a leader at random.
	leader := selectRandomReplica(replicas)

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
	future := m.raft.Apply(data, raftApplyTimeout)
	if err := future.Error(); err != nil {
		return status.New(codes.Internal, "Failed to replicate stream")
	}

	// If there is a response, it's an ErrStreamExists.
	if resp := future.Response(); resp != nil {
		err := resp.(error)
		return status.New(codes.AlreadyExists, err.Error())
	}

	return nil
}

func (m *metadataAPI) ShrinkISR(ctx context.Context, req *proto.ShrinkISROp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.isLeader() {
		return m.propagateShrinkISR(ctx, req)
	}

	// Verify the stream exists.
	stream := m.GetStream(req.Subject, req.Name)
	if stream == nil {
		return status.New(codes.FailedPrecondition, fmt.Sprintf("No such stream [subject=%s, name=%s]",
			req.Subject, req.Name))
	}

	// Check the leader epoch.
	leader, epoch := stream.GetLeader()
	if req.Leader != leader || req.LeaderEpoch != epoch {
		return status.New(
			codes.FailedPrecondition,
			fmt.Sprintf("Leader generation mismatch, current leader: %s epoch: %d, got leader: %s epoch: %d",
				leader, epoch, req.Leader, req.LeaderEpoch))
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

func (m *metadataAPI) ExpandISR(ctx context.Context, req *proto.ExpandISROp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.isLeader() {
		return m.propagateExpandISR(ctx, req)
	}

	// Verify the stream exists.
	stream := m.GetStream(req.Subject, req.Name)
	if stream == nil {
		return status.New(codes.FailedPrecondition, fmt.Sprintf("No such stream [subject=%s, name=%s]",
			req.Subject, req.Name))
	}

	// Check the leader epoch.
	leader, epoch := stream.GetLeader()
	if req.Leader != leader || req.LeaderEpoch != epoch {
		return status.New(
			codes.FailedPrecondition,
			fmt.Sprintf("Leader generation mismatch, current leader: %s epoch: %d, got leader: %s epoch: %d",
				leader, epoch, req.Leader, req.LeaderEpoch))
	}

	// Replicate ISR expand through Raft.
	op := &proto.RaftLog{
		Op:          proto.Op_EXPAND_ISR,
		ExpandISROp: req,
	}

	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}

	// Wait on result of replication.
	if err := m.raft.Apply(data, raftApplyTimeout).Error(); err != nil {
		return status.New(codes.Internal, "Failed to expand ISR")
	}

	return nil
}

func (m *metadataAPI) ReportLeader(ctx context.Context, req *proto.ReportLeaderOp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.isLeader() {
		return m.propagateReportLeader(ctx, req)
	}

	// Verify the stream exists.
	stream := m.GetStream(req.Subject, req.Name)
	if stream == nil {
		return status.New(codes.FailedPrecondition, fmt.Sprintf("No such stream [subject=%s, name=%s]",
			req.Subject, req.Name))
	}

	m.mu.Lock()
	reported := m.leaderReports[stream]
	if reported == nil {
		reported = &leaderReported{
			stream:          stream,
			witnessReplicas: make(map[string]struct{}),
			api:             m,
		}
		m.leaderReports[stream] = reported
	}
	m.mu.Unlock()

	return reported.addWitness(req.Replica)
}

func (m *metadataAPI) AddStream(protoStream *proto.Stream, recovered bool) (*stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	streams := m.streams[protoStream.Subject]
	if streams == nil {
		streams = make(subjectStreams)
		m.streams[protoStream.Subject] = streams
	}
	if _, ok := streams[protoStream.Name]; ok {
		// Stream for subject with name already exists.
		return nil, ErrStreamExists
	}

	// This will initialize/recover the durable commit log.
	stream, err := m.newStream(protoStream, recovered)
	if err != nil {
		return nil, err
	}

	streams[stream.Name] = stream

	// Start leader/follower loop if necessary.
	leader, epoch := stream.GetLeader()
	err = stream.SetLeader(leader, epoch)
	return stream, err
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
	for _, report := range m.leaderReports {
		report.cancel()
	}
	m.leaderReports = make(map[*stream]*leaderReported)
	return nil
}

func (m *metadataAPI) LostLeadership() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, report := range m.leaderReports {
		report.cancel()
	}
	m.leaderReports = make(map[*stream]*leaderReported)
}

// getStreamReplicas selects replicationFactor replicas to participate in the
// stream.
func (m *metadataAPI) getStreamReplicas(replicationFactor int32) ([]string, *status.Status) {
	// TODO: Currently this selection is random but could be made more
	// intelligent, e.g. selecting based on current load.
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

// electNewStreamLeader selects a new leader for the given stream, applies this
// update to the Raft group, and notifies the replica set. This will fail if
// the current broker is not the metadata leader.
func (m *metadataAPI) electNewStreamLeader(stream *stream) *status.Status {
	isr := stream.GetISR()
	if len(isr) == 1 {
		return status.New(codes.FailedPrecondition, "No ISR candidates")
	}
	var (
		candidates = make([]string, 0, len(isr)-1)
		leader, _  = stream.GetLeader()
	)
	for _, candidate := range isr {
		if candidate == leader {
			continue
		}
		candidates = append(candidates, candidate)
	}

	if len(candidates) == 0 {
		return status.New(codes.FailedPrecondition, "No ISR candidates")
	}

	// Select a new leader at random.
	leader = selectRandomReplica(candidates)

	// Replicate leader change through Raft.
	op := &proto.RaftLog{
		Op: proto.Op_CHANGE_LEADER,
		ChangeLeaderOp: &proto.ChangeLeaderOp{
			Subject: stream.Subject,
			Name:    stream.Name,
			Leader:  leader,
		},
	}

	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}

	// Wait on result of replication.
	if err := m.raft.Apply(data, raftApplyTimeout).Error(); err != nil {
		return status.New(codes.Internal, "Failed to replicate leader change")
	}

	return nil
}

func (m *metadataAPI) propagateCreateStream(ctx context.Context, req *client.CreateStreamRequest) *status.Status {
	propagate := &proto.PropagatedRequest{
		Op:             proto.Op_CREATE_STREAM,
		CreateStreamOp: req,
	}
	return m.propagateRequest(ctx, propagate)
}

func (m *metadataAPI) propagateShrinkISR(ctx context.Context, req *proto.ShrinkISROp) *status.Status {
	propagate := &proto.PropagatedRequest{
		Op:          proto.Op_SHRINK_ISR,
		ShrinkISROp: req,
	}
	return m.propagateRequest(ctx, propagate)
}

func (m *metadataAPI) propagateExpandISR(ctx context.Context, req *proto.ExpandISROp) *status.Status {
	propagate := &proto.PropagatedRequest{
		Op:          proto.Op_EXPAND_ISR,
		ExpandISROp: req,
	}
	return m.propagateRequest(ctx, propagate)
}

func (m *metadataAPI) propagateReportLeader(ctx context.Context, req *proto.ReportLeaderOp) *status.Status {
	propagate := &proto.PropagatedRequest{
		Op:             proto.Op_REPORT_LEADER,
		ReportLeaderOp: req,
	}
	return m.propagateRequest(ctx, propagate)
}

func (m *metadataAPI) propagateRequest(ctx context.Context, req *proto.PropagatedRequest) *status.Status {
	data, err := req.Marshal()
	if err != nil {
		panic(err)
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultPropagateTimeout)
		defer cancel()
	}

	resp, err := m.nc.RequestWithContext(ctx, m.getPropagateInbox(), data)
	if err != nil {
		return status.New(codes.Internal, err.Error())
	}

	r := &proto.PropagatedResponse{}
	if err := r.Unmarshal(resp.Data); err != nil {
		m.logger.Errorf("metadata: Invalid response for propagated request: %v", err)
		return status.New(codes.Internal, "invalid response")
	}
	if r.Error != nil {
		return status.New(codes.Code(r.Error.Code), r.Error.Msg)
	}

	return nil
}

// selectRandomReplica selects a random replica from the list of replicas.
func selectRandomReplica(replicas []string) string {
	return replicas[rand.Intn(len(replicas))]
}
