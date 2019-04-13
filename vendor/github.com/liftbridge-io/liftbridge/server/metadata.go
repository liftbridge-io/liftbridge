package server

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	client "github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liftbridge-io/liftbridge/server/proto"
)

const (
	defaultPropagateTimeout       = 5 * time.Second
	maxReplicationFactor    int32 = -1
)

// ErrStreamExists is returned by CreateStream when attempting to create a
// stream that already has the provided subject and name.
var ErrStreamExists = errors.New("stream already exists")

// subjectStreams maps a name to a stream within the scope of a subject.
type subjectStreams map[string]*stream

// leaderReport tracks witnesses for a stream leader. Witnesses are replicas
// which have reported the leader as unresponsive. If a quorum of replicas
// report the leader within a bounded period of time, the controller will
// select a new leader.
type leaderReport struct {
	mu              sync.Mutex
	stream          *stream
	timer           *time.Timer
	witnessReplicas map[string]struct{}
	api             *metadataAPI
}

// addWitness adds the given replica to the leaderReport witnesses. If a quorum
// of replicas have reported the leader, a new leader will be selected.
// Otherwise, the expiration timer is reset. An error is returned if selecting
// a new leader fails.
func (l *leaderReport) addWitness(replica string) *status.Status {
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

// cancel stops the expiration timer, if there is one.
func (l *leaderReport) cancel() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.timer != nil {
		l.timer.Stop()
	}
}

// metadataAPI is the internal API for interacting with cluster data. All
// stream access should go through the exported methods of the metadataAPI.
type metadataAPI struct {
	*Server
	streams         map[string]subjectStreams
	mu              sync.RWMutex
	leaderReports   map[*stream]*leaderReport
	cachedBrokers   []*client.Broker
	cachedServerIDs map[string]struct{}
	lastCached      time.Time
}

func newMetadataAPI(s *Server) *metadataAPI {
	return &metadataAPI{
		Server:        s,
		streams:       make(map[string]subjectStreams),
		leaderReports: make(map[*stream]*leaderReport),
	}
}

// FetchMetadata retrieves the cluster metadata for the given request. If the
// request specifies streams, it will only return metadata for those particular
// streams. If not, it will return metadata for all streams.
func (m *metadataAPI) FetchMetadata(ctx context.Context, req *client.FetchMetadataRequest) (
	*client.FetchMetadataResponse, *status.Status) {

	resp := m.createMetadataResponse(req.Streams)

	servers, err := m.getClusterServerIDs()
	if err != nil {
		return nil, status.New(codes.Internal, err.Error())
	}

	serverIDs := make(map[string]struct{}, len(servers))
	for _, id := range servers {
		serverIDs[id] = struct{}{}
	}

	// Check if we can use cached broker info.
	if cached, ok := m.brokerCache(serverIDs); ok {
		resp.Brokers = cached
	} else {
		// Query broker info from peers.
		brokers, err := m.fetchBrokerInfo(ctx, len(servers)-1)
		if err != nil {
			return nil, err
		}
		resp.Brokers = brokers

		// Update the cache.
		m.mu.Lock()
		m.cachedBrokers = brokers
		m.cachedServerIDs = serverIDs
		m.lastCached = time.Now()
		m.mu.Unlock()
	}

	return resp, nil
}

// brokerCache checks if the cache of broker metadata is clean and, if it is
// and it's not past the metadata cache max age, returns the cached broker
// list. The bool returned indicates if the cached data is returned or not.
func (m *metadataAPI) brokerCache(serverIDs map[string]struct{}) ([]*client.Broker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	serversChanged := false
	if len(serverIDs) != len(m.cachedServerIDs) {
		serversChanged = true
	} else {
		for id := range serverIDs {
			if _, ok := m.cachedServerIDs[id]; !ok {
				serversChanged = true
				break
			}
		}
	}
	useCache := len(m.cachedBrokers) > 0 &&
		!serversChanged &&
		time.Since(m.lastCached) <= m.config.MetadataCacheMaxAge
	if useCache {
		return m.cachedBrokers, true
	}
	return nil, false
}

// fetchBrokerInfo retrieves the broker metadata for the cluster. The numPeers
// argument is the expected number of peers to get a response from.
func (m *metadataAPI) fetchBrokerInfo(ctx context.Context, numPeers int) ([]*client.Broker, *status.Status) {
	// Add ourselves.
	brokers := []*client.Broker{&client.Broker{
		Id:   m.config.Clustering.ServerID,
		Host: m.config.Host,
		Port: int32(m.config.Port),
	}}

	// Make sure there is a deadline on the request.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultPropagateTimeout)
		defer cancel()
	}

	// Create subscription to receive responses on.
	inbox := nats.NewInbox()
	sub, err := m.ncRaft.SubscribeSync(inbox)
	if err != nil {
		return nil, status.New(codes.Internal, err.Error())
	}
	defer sub.Unsubscribe()

	// Survey the cluster.
	queryReq, err := (&proto.ServerInfoRequest{
		Id: m.config.Clustering.ServerID,
	}).Marshal()
	if err != nil {
		panic(err)
	}
	m.ncRaft.PublishRequest(m.serverInfoInbox(), inbox, queryReq)

	// Gather responses.
	for i := 0; i < numPeers; i++ {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			break
		}
		queryResp := &proto.ServerInfoResponse{}
		if err := queryResp.Unmarshal(msg.Data); err != nil {
			m.logger.Warnf("Received invalid server info response: %v", err)
			continue
		}
		brokers = append(brokers, &client.Broker{
			Id:   queryResp.Id,
			Host: queryResp.Host,
			Port: queryResp.Port,
		})
	}

	return brokers, nil
}

// createMetadataResponse creates a FetchMetadataResponse and populates it with
// stream metadata. If the provided list of StreamDescriptors is empty, it will
// populate metadata for all streams. Otherwise, it populates only the
// specified streams.
func (m *metadataAPI) createMetadataResponse(streams []*client.StreamDescriptor) *client.FetchMetadataResponse {
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

	return &client.FetchMetadataResponse{Metadata: metadata}
}

// CreateStream creates a new stream if this server is the metadata leader. If
// it is not, it will forward the request to the leader and return the
// response. This operation is replicated by Raft. The metadata leader will
// select replicationFactor nodes to participate in the stream and a leader. If
// successful, this will return once the stream has been replicated to the
// cluster and the stream leader has started.
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
				Group:             req.Group,
				ReplicationFactor: req.ReplicationFactor,
				Replicas:          replicas,
				Leader:            leader,
				Isr:               replicas,
			},
		},
	}

	// Wait on result of replication.
	future := m.applyRaftOperation(op)
	if err := future.Error(); err != nil {
		return status.New(codes.Internal, "Failed to replicate stream")
	}

	// If there is a response, it's an ErrStreamExists.
	if resp := future.Response(); resp != nil {
		err := resp.(error)
		return status.New(codes.AlreadyExists, err.Error())
	}

	// Wait for leader to create stream (best effort).
	m.waitForStreamLeader(ctx, req.Subject, req.Name, leader)

	return nil
}

// ShrinkISR removes the specified replica from the stream's in-sync replicas
// set if this server is the metadata leader. If it is not, it will forward the
// request to the leader and return the response. This operation is replicated
// by Raft.
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

	// Wait on result of replication.
	if err := m.applyRaftOperation(op).Error(); err != nil {
		return status.New(codes.Internal, "Failed to shrink ISR")
	}

	return nil
}

// ExpandISR adds the specified replica to the stream's in-sync replicas set if
// this server is the metadata leader. If it is not, it will forward the
// request to the leader and return the response. This operation is replicated
// by Raft.
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

	// Wait on result of replication.
	if err := m.applyRaftOperation(op).Error(); err != nil {
		return status.New(codes.Internal, "Failed to expand ISR")
	}

	return nil
}

// ReportLeader marks the stream leader as unresponsive with respect to the
// specified replica if this server is the metadata leader. If it is not, it
// will forward the request to the leader and return the response. If a quorum
// of replicas report the stream leader within a bounded period, the metadata
// leader will select a new stream leader.
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

	// Check the leader epoch.
	leader, epoch := stream.GetLeader()
	if req.Leader != leader || req.LeaderEpoch != epoch {
		return status.New(
			codes.FailedPrecondition,
			fmt.Sprintf("Leader generation mismatch, current leader: %s epoch: %d, got leader: %s epoch: %d",
				leader, epoch, req.Leader, req.LeaderEpoch))
	}

	m.mu.Lock()
	reported := m.leaderReports[stream]
	if reported == nil {
		reported = &leaderReport{
			stream:          stream,
			witnessReplicas: make(map[string]struct{}),
			api:             m,
		}
		m.leaderReports[stream] = reported
	}
	m.mu.Unlock()

	return reported.addWitness(req.Replica)
}

// AddStream adds the given stream to the metadata store. It returns
// ErrStreamExists if there already exists a stream with the given subject and
// name. If the stream is recovered, this will not start the stream until
// recovery completes.
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

// GetStreams returns all streams from the metadata store.
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

// GetStream returns the stream with the given subject and name. It returns nil
// if no such stream exists.
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

// Reset closes all streams and clears all existing state in the metadata
// store.
func (m *metadataAPI) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, streams := range m.streams {
		for _, stream := range streams {
			if err := stream.Close(); err != nil {
				return err
			}
		}
	}
	m.streams = make(map[string]subjectStreams)
	for _, report := range m.leaderReports {
		report.cancel()
	}
	m.leaderReports = make(map[*stream]*leaderReport)
	return nil
}

// LostLeadership should be called when the server loses metadata leadership.
func (m *metadataAPI) LostLeadership() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, report := range m.leaderReports {
		report.cancel()
	}
	m.leaderReports = make(map[*stream]*leaderReport)
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
	if replicationFactor == maxReplicationFactor {
		replicationFactor = int32(len(ids))
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

// getClusterServerIDs returns a list of all the broker IDs in the cluster.
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
	// TODO: add support for "unclean" leader elections.
	if len(isr) <= 1 {
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

	// Wait on result of replication.
	if err := m.applyRaftOperation(op).Error(); err != nil {
		return status.New(codes.Internal, "Failed to replicate leader change")
	}

	return nil
}

// propagateCreateStream forwards a CreateStream request to the metadata leader
// and returns the response.
func (m *metadataAPI) propagateCreateStream(ctx context.Context, req *client.CreateStreamRequest) *status.Status {
	propagate := &proto.PropagatedRequest{
		Op:             proto.Op_CREATE_STREAM,
		CreateStreamOp: req,
	}
	return m.propagateRequest(ctx, propagate)
}

// propagateShrinkISR forwards a ShrinkISR request to the metadata leader and
// returns the response.
func (m *metadataAPI) propagateShrinkISR(ctx context.Context, req *proto.ShrinkISROp) *status.Status {
	propagate := &proto.PropagatedRequest{
		Op:          proto.Op_SHRINK_ISR,
		ShrinkISROp: req,
	}
	return m.propagateRequest(ctx, propagate)
}

// propagateExpandISR forwards a ExpandISR request to the metadata leader and
// returns the response.
func (m *metadataAPI) propagateExpandISR(ctx context.Context, req *proto.ExpandISROp) *status.Status {
	propagate := &proto.PropagatedRequest{
		Op:          proto.Op_EXPAND_ISR,
		ExpandISROp: req,
	}
	return m.propagateRequest(ctx, propagate)
}

// propagateReportLeader forwards a ReportLeader request to the metadata leader
// and returns the response.
func (m *metadataAPI) propagateReportLeader(ctx context.Context, req *proto.ReportLeaderOp) *status.Status {
	propagate := &proto.PropagatedRequest{
		Op:             proto.Op_REPORT_LEADER,
		ReportLeaderOp: req,
	}
	return m.propagateRequest(ctx, propagate)
}

// propagateRequest forwards a metadata request to the metadata leader and
// returns the response.
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

// waitForStreamLeader does a best-effort wait for the leader of the given
// stream to create and start the stream.
func (m *metadataAPI) waitForStreamLeader(ctx context.Context, subject, name, leader string) {
	if leader == m.config.Clustering.ServerID {
		// If we're the stream leader, there's no need to make a status
		// request. We can just apply a Raft barrier since the FSM is local.
		if err := m.raft.Barrier(5 * time.Second).Error(); err != nil {
			m.logger.Warnf("Failed to apply Raft barrier: %v", err)
		}
		return
	}

	req, err := (&proto.StreamStatusRequest{
		Subject: subject,
		Name:    name,
	}).Marshal()
	if err != nil {
		panic(err)
	}
	inbox := fmt.Sprintf(streamStatusInboxTemplate, m.baseMetadataRaftSubject(), leader)
	for i := 0; i < 5; i++ {
		resp, err := m.ncRaft.RequestWithContext(ctx, inbox, req)
		if err != nil {
			m.logger.Warnf(
				"Failed to get status for stream [subject=%s, name=%s] from leader %s: %v",
				subject, name, leader, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		statusResp := &proto.StreamStatusResponse{}
		if err := statusResp.Unmarshal(resp.Data); err != nil {
			m.logger.Warnf(
				"Invalid status response for stream [subject=%s, name=%s] from leader %s: %v",
				subject, name, leader, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if !statusResp.Exists || !statusResp.IsLeader {
			// The leader hasn't finished creating the stream, so wait a bit
			// and retry.
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
}

// applyRaftOperation proposes the given operation to the Raft cluster. This
// should only be called when the server is metadata leader. However, if the
// server has lost leadership, the returned future will yield an error.
func (m *metadataAPI) applyRaftOperation(op *proto.RaftLog) raft.ApplyFuture {
	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}
	return m.raft.Apply(data, raftApplyTimeout)
}

// selectRandomReplica selects a random replica from the list of replicas.
func selectRandomReplica(replicas []string) string {
	return replicas[rand.Intn(len(replicas))]
}
