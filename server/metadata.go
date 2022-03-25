package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	client "github.com/liftbridge-io/liftbridge-api/go"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

const (
	defaultPropagateTimeout             = 5 * time.Second
	defaultFetchBrokerInfoTimeout       = 3 * time.Second
	maxReplicationFactor          int32 = -1
)

var (
	// ErrStreamExists is returned by CreateStream when attempting to create a
	// stream that already exists.
	ErrStreamExists = errors.New("stream already exists")

	// ErrStreamNotFound is returned by DeleteStream/PauseStream when
	// attempting to delete/pause a stream that does not exist.
	ErrStreamNotFound = errors.New("stream does not exist")

	// ErrPartitionNotFound is returned by PauseStream when attempting to pause
	// a stream partition that does not exist.
	ErrPartitionNotFound = errors.New("partition does not exist")

	// ErrConsumerGroupExists is returned by createConsumerGroup when
	// attempting to create a group that already exists.
	ErrConsumerGroupExists = errors.New("consumer group already exists")

	// ErrConsumerGroupNotFound is returned by JoinConsumerGroup when
	// attempting to join a group that does not exist.
	ErrConsumerGroupNotFound = errors.New("consumer group does not exist")

	// ErrConsumerAlreadyMember is returned by JoinConsumerGroup when the
	// consumer is already a member of the group.
	ErrConsumerAlreadyMember = errors.New("consumer is already a member of the consumer group")

	// ErrConsumerNotMember is returned by LeaveConsumerGroup when the
	// consumer if not a member of the group.
	ErrConsumerNotMember = errors.New("consumer is not a member of the consumer group")

	// ErrBrokerNotCoordinator is returned by GetConsumerGroupAssignments when
	// this server is not the coordinator for the requested consumer group.
	ErrBrokerNotCoordinator = errors.New("broker is not the consumer group coordinator")

	// ErrGroupEpoch is returned by GetConsumerGroupAssignments when the
	// client-provided group epoch differs from the server-side group epoch.
	ErrGroupEpoch = errors.New("client-provided group epoch differs from broker group epoch")
)

// metadataAPI is the internal API for interacting with cluster data. All
// stream access should go through the exported methods of the metadataAPI.
type metadataAPI struct {
	*Server
	streams            map[string]*stream
	mu                 sync.RWMutex
	partitionFailovers map[*partition]*failoverStatus
	cachedBrokers      []*client.Broker
	cachedServerIDs    map[string]struct{}
	lastCached         time.Time
	consumerGroupsMu   sync.RWMutex
	consumerGroups     map[string]*consumerGroup
	groupFailovers     map[*consumerGroup]*failoverStatus
	stats              struct {
		sync.RWMutex
		brokerLeaderLoad      map[string]int
		brokerPartitionLoad   map[string]int
		brokerCoordinatorLoad map[string]int
	}
}

func newMetadataAPI(s *Server) *metadataAPI {
	m := &metadataAPI{
		Server:             s,
		streams:            make(map[string]*stream),
		partitionFailovers: make(map[*partition]*failoverStatus),
		consumerGroups:     make(map[string]*consumerGroup),
		groupFailovers:     make(map[*consumerGroup]*failoverStatus),
	}
	m.stats.brokerLeaderLoad = make(map[string]int)
	m.stats.brokerPartitionLoad = make(map[string]int)
	m.stats.brokerCoordinatorLoad = make(map[string]int)
	return m
}

// BrokerPartitionCounts returns a map of broker IDs to the number of
// partitions they are hosting.
func (m *metadataAPI) BrokerPartitionCounts() map[string]int {
	m.stats.RLock()
	counts := make(map[string]int, len(m.stats.brokerPartitionLoad))
	for broker, count := range m.stats.brokerPartitionLoad {
		counts[broker] = count
	}
	m.stats.RUnlock()
	return counts
}

// BrokerLeaderCounts returns a map of broker IDs to the number of
// partitions they are leading.
func (m *metadataAPI) BrokerLeaderCounts() map[string]int {
	m.stats.RLock()
	counts := make(map[string]int, len(m.stats.brokerLeaderLoad))
	for broker, count := range m.stats.brokerLeaderLoad {
		counts[broker] = count
	}
	m.stats.RUnlock()
	return counts
}

// FetchMetadata retrieves the cluster metadata for the given request. If the
// request specifies streams, it will only return metadata for those particular
// streams. If not, it will return metadata for all streams.
func (m *metadataAPI) FetchMetadata(ctx context.Context, req *client.FetchMetadataRequest) (
	*client.FetchMetadataResponse, *status.Status) {

	resp := m.createMetadataResponse(req.Streams, req.Groups)

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

// FetchPartitionMetadata retrieves the metadata for the partition leader. This
// mainly serves the purpose of returning high watermark and newest offset.
func (m *metadataAPI) FetchPartitionMetadata(ctx context.Context, req *client.FetchPartitionMetadataRequest) (
	*client.FetchPartitionMetadataResponse, *status.Status) {

	partition := m.GetPartition(req.Stream, req.Partition)
	if partition == nil {
		return nil, status.New(codes.NotFound, "partition not found")
	}
	if !partition.IsLeader() {
		return nil, status.New(codes.FailedPrecondition, "The request should be sent to partition leader")
	}
	metadata := getPartitionMetadata(req.Partition, partition)
	return &client.FetchPartitionMetadataResponse{Metadata: metadata}, nil
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
	// Brokers load data
	partitionCountMap := m.BrokerPartitionCounts()
	partitionLeaderCountMap := m.BrokerLeaderCounts()

	// Add ourselves.
	connectionAddress := m.getConnectionAddress()
	brokers := []*client.Broker{{
		Id:             m.config.Clustering.ServerID,
		Host:           connectionAddress.Host,
		Port:           int32(connectionAddress.Port),
		PartitionCount: int32(partitionCountMap[m.config.Clustering.ServerID]),
		LeaderCount:    int32(partitionLeaderCountMap[m.config.Clustering.ServerID]),
	}}

	// Make sure there is a deadline on the request.
	ctx, cancel := ensureTimeout(ctx, defaultFetchBrokerInfoTimeout)
	defer cancel()

	// Create subscription to receive responses on.
	inbox := m.getMetadataReplyInbox()
	sub, err := m.ncRaft.SubscribeSync(inbox)
	if err != nil {
		return nil, status.New(codes.Internal, err.Error())
	}
	defer sub.Unsubscribe()

	// Survey the cluster.
	queryReq, err := proto.MarshalServerInfoRequest(&proto.ServerInfoRequest{
		Id: m.config.Clustering.ServerID,
	})
	if err != nil {
		panic(err)
	}
	if err := m.ncRaft.PublishRequest(m.getServerInfoInbox(), inbox, queryReq); err != nil {
		return nil, status.New(codes.Internal, err.Error())
	}

	// Gather responses.
	for i := 0; i < numPeers; i++ {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			break
		}
		queryResp, err := proto.UnmarshalServerInfoResponse(msg.Data)
		if err != nil {
			m.logger.Warnf("Received invalid server info response: %v", err)
			continue
		}
		brokers = append(brokers, &client.Broker{
			Id:             queryResp.Id,
			Host:           queryResp.Host,
			Port:           queryResp.Port,
			PartitionCount: int32(partitionCountMap[queryResp.Id]),
			LeaderCount:    int32(partitionLeaderCountMap[queryResp.Id]),
		})
	}

	return brokers, nil
}

// createMetadataResponse creates a FetchMetadataResponse and populates it with
// stream and group metadata. If the provided list of stream names is empty, it
// will populate metadata for all streams. Otherwise, it populates only the
// specified streams.
func (m *metadataAPI) createMetadataResponse(streams, groups []string) *client.FetchMetadataResponse {
	// If no stream names were provided, fetch metadata for all streams.
	if len(streams) == 0 {
		for _, stream := range m.GetStreams() {
			streams = append(streams, stream.GetName())
		}
	}

	streamMetadata := make([]*client.StreamMetadata, len(streams))

	for i, name := range streams {
		stream := m.GetStream(name)
		if stream == nil {
			// Stream does not exist.
			streamMetadata[i] = &client.StreamMetadata{
				Name:  name,
				Error: client.StreamMetadata_UNKNOWN_STREAM,
			}
		} else {
			partitions := make(map[int32]*client.PartitionMetadata)
			for id, partition := range stream.GetPartitions() {
				partitions[id] = getPartitionMetadata(id, partition)
			}
			streamMetadata[i] = &client.StreamMetadata{
				Name:              name,
				Subject:           stream.GetSubject(),
				Partitions:        partitions,
				CreationTimestamp: stream.GetCreationTime().UnixNano(),
			}
		}
	}

	groupMetadata := make([]*client.ConsumerGroupMetadata, len(groups))
	for i, id := range groups {
		group := m.GetConsumerGroup(id)
		if group == nil {
			// Group does not exist.
			groupMetadata[i] = &client.ConsumerGroupMetadata{
				GroupId: id,
				Error:   client.ConsumerGroupMetadata_UNKNOWN_GROUP,
			}
		} else {
			coordinator, epoch := group.GetCoordinator()
			groupMetadata[i] = &client.ConsumerGroupMetadata{
				GroupId:     id,
				Coordinator: coordinator,
				Epoch:       epoch,
			}
		}
	}

	return &client.FetchMetadataResponse{
		StreamMetadata: streamMetadata,
		GroupMetadata:  groupMetadata,
	}
}

// CreateStream creates a new stream if this server is the metadata leader. If
// it is not, it will forward the request to the leader and return the
// response. This operation is replicated by Raft. The metadata leader will
// select replicationFactor nodes to participate and a leader for each
// partition.  If successful, this will return once the partitions have been
// replicated to the cluster and the partition leaders have started.
func (m *metadataAPI) CreateStream(ctx context.Context, req *proto.CreateStreamOp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		isLeader, st := m.propagateCreateStream(ctx, req)
		if st != nil {
			return st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return nil
		}
	}

	if len(req.Stream.Partitions) == 0 {
		return status.New(codes.InvalidArgument, "no partitions provided")
	}

	for _, partition := range req.Stream.Partitions {
		// Select replicationFactor nodes to participate in the partition.
		replicas, st := m.getPartitionReplicas(partition.ReplicationFactor)
		if st != nil {
			return st
		}

		// Select a leader for the partition.
		leader := m.selectPartitionLeader(replicas)

		partition.Replicas = replicas
		partition.Isr = replicas
		partition.Leader = leader
	}

	req.Stream.CreationTimestamp = time.Now().UnixNano()

	// Replicate stream create through Raft.
	op := &proto.RaftLog{
		Op:             proto.Op_CREATE_STREAM,
		CreateStreamOp: req,
	}

	// Wait on result of replication.
	future, err := m.getRaft().applyOperation(ctx, op, m.checkCreateStreamPreconditions)
	if err != nil {
		code := codes.FailedPrecondition
		if err == ErrStreamExists {
			code = codes.AlreadyExists
		}
		return status.Newf(code, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to replicate partition: %v", err.Error())
	}

	// Wait for leaders to create partitions (best effort).
	var wg sync.WaitGroup
	wg.Add(len(req.Stream.Partitions))
	for _, partition := range req.Stream.Partitions {
		m.startGoroutineWithArgs(func(args ...interface{}) {
			m.waitForPartitionLeader(ctx, args[0].(*proto.Partition))
			wg.Done()
		}, partition)
	}
	wg.Wait()

	return nil
}

// DeleteStream deletes a stream if this server is the metadata leader. If it is
// not, it will forward the request to the leader and return the response. This
// operation is replicated by Raft. If successful, this will return once the
// stream has been deleted from the cluster.
func (m *metadataAPI) DeleteStream(ctx context.Context, req *proto.DeleteStreamOp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		isLeader, st := m.propagateDeleteStream(ctx, req)
		if st != nil {
			return st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return nil
		}
	}

	// Replicate partition deletion through Raft.
	op := &proto.RaftLog{
		Op:             proto.Op_DELETE_STREAM,
		DeleteStreamOp: req,
	}

	// Wait on result of deletion.
	future, err := m.getRaft().applyOperation(ctx, op, m.checkDeleteStreamPreconditions)
	if err != nil {
		code := codes.FailedPrecondition
		if err == ErrStreamNotFound {
			code = codes.NotFound
		}
		return status.Newf(code, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to delete stream: %v", err.Error())
	}

	return nil
}

// PauseStream pauses a stream if this server is the metadata leader. If it is
// not, it will forward the request to the leader and return the response. This
// operation is replicated by Raft. If successful, this will return once the
// stream has been paused.
func (m *metadataAPI) PauseStream(ctx context.Context, req *proto.PauseStreamOp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		isLeader, st := m.propagatePauseStream(ctx, req)
		if st != nil {
			return st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return nil
		}
	}

	// Replicate stream pausing through Raft.
	op := &proto.RaftLog{
		Op:            proto.Op_PAUSE_STREAM,
		PauseStreamOp: req,
	}

	// Wait on result of pausing.
	future, err := m.getRaft().applyOperation(ctx, op, m.checkPauseStreamPreconditions)
	if err != nil {
		code := codes.FailedPrecondition
		if err == ErrStreamNotFound || err == ErrPartitionNotFound {
			code = codes.NotFound
		}
		return status.Newf(code, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to pause stream: %v", err.Error())
	}

	return nil
}

// ResumeStream unpauses a stream partition(s) if this server is the metadata
// leader. If it is not, it will forward the request to the leader and return
// the response. This operation is replicated by Raft. Resume is intended to
// be idempotent. If the partition is already resumed when this is called, this
// will return nil. If the partition to resume is not specified on the request,
// this will resume all paused partitions in the stream.
func (m *metadataAPI) ResumeStream(ctx context.Context, req *proto.ResumeStreamOp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		isLeader, st := m.propagateResumeStream(ctx, req)
		if st != nil {
			return st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return nil
		}
	}

	// Replicate stream resume through Raft.
	op := &proto.RaftLog{
		Op:             proto.Op_RESUME_STREAM,
		ResumeStreamOp: req,
	}

	// Wait on result of replication.
	future, err := m.getRaft().applyOperation(ctx, op, m.checkResumeStreamPreconditions)
	if err != nil {
		code := codes.FailedPrecondition
		if err == ErrStreamNotFound || err == ErrPartitionNotFound {
			code = codes.NotFound
		}
		return status.Newf(code, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to resume stream: %v", err.Error())
	}

	// Wait for leader to resume partition(s) (best effort).
	var wg sync.WaitGroup
	wg.Add(len(req.Partitions))
	for _, partitionID := range req.Partitions {
		partition := m.GetPartition(req.Stream, partitionID)
		m.startGoroutineWithArgs(func(args ...interface{}) {
			m.waitForPartitionLeader(ctx, args[0].(*proto.Partition))
			wg.Done()
		}, partition.Partition)
	}
	wg.Wait()

	return nil
}

// ShrinkISR removes the specified replica from the partition's in-sync
// replicas set if this server is the metadata leader. If it is not, it will
// forward the request to the leader and return the response. This operation is
// replicated by Raft.
func (m *metadataAPI) ShrinkISR(ctx context.Context, req *proto.ShrinkISROp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		isLeader, st := m.propagateShrinkISR(ctx, req)
		if st != nil {
			return st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return nil
		}
	}

	// Verify the partition exists.
	partition := m.GetPartition(req.Stream, req.Partition)
	if partition == nil {
		return status.New(codes.FailedPrecondition, fmt.Sprintf("No such partition [stream=%s, partition=%d]",
			req.Stream, req.Partition))
	}

	// Check the leader epoch.
	leader, epoch := partition.GetLeader()
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
	future, err := m.getRaft().applyOperation(ctx, op, m.checkShrinkISRPreconditions)
	if err != nil {
		return status.Newf(codes.FailedPrecondition, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to shrink ISR: %v", err.Error())
	}

	return nil
}

// ExpandISR adds the specified replica to the partition's in-sync replicas set
// if this server is the metadata leader. If it is not, it will forward the
// request to the leader and return the response. This operation is replicated
// by Raft.
func (m *metadataAPI) ExpandISR(ctx context.Context, req *proto.ExpandISROp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		isLeader, st := m.propagateExpandISR(ctx, req)
		if st != nil {
			return st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return nil
		}
	}

	// Verify the partition exists.
	partition := m.GetPartition(req.Stream, req.Partition)
	if partition == nil {
		return status.New(codes.FailedPrecondition, fmt.Sprintf("No such partition [stream=%s, partition=%d]",
			req.Stream, req.Partition))
	}

	// Check the leader epoch.
	leader, epoch := partition.GetLeader()
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
	future, err := m.getRaft().applyOperation(ctx, op, m.checkExpandISRPreconditions)
	if err != nil {
		return status.Newf(codes.FailedPrecondition, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to expand ISR: %v", err.Error())
	}

	return nil
}

// ReportLeader marks the partition leader as unresponsive with respect to the
// specified replica if this server is the metadata leader. If it is not, it
// will forward the request to the leader and return the response. If a quorum
// of replicas report the partition leader within a bounded period, the
// metadata leader will select a new partition leader.
func (m *metadataAPI) ReportLeader(ctx context.Context, req *proto.ReportLeaderOp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		isLeader, st := m.propagateReportLeader(ctx, req)
		if st != nil {
			return st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return nil
		}
	}

	// Verify the partition exists.
	partition := m.GetPartition(req.Stream, req.Partition)
	if partition == nil {
		return status.New(codes.FailedPrecondition, fmt.Sprintf("No such partition [stream=%s, partition=%d]",
			req.Stream, req.Partition))
	}

	// Check the leader epoch.
	leader, epoch := partition.GetLeader()
	if req.Leader != leader || req.LeaderEpoch != epoch {
		return status.New(
			codes.FailedPrecondition,
			fmt.Sprintf("Leader generation mismatch, current leader: %s epoch: %d, got leader: %s epoch: %d",
				leader, epoch, req.Leader, req.LeaderEpoch))
	}

	m.mu.Lock()
	failover := m.partitionFailovers[partition]
	if failover == nil {
		failover = newPartitionFailoverStatus(
			partition,
			m.config.Clustering.ReplicaMaxLeaderTimeout,
			m.newPartitionFailoverExpiredHandler(partition),
			m.newPartitionFailoverHandler(partition),
		)
		m.partitionFailovers[partition] = failover
	}
	m.mu.Unlock()

	return failover.report(ctx, req.Replica)
}

func (m *metadataAPI) newPartitionFailoverExpiredHandler(p *partition) failoverExpiredHandler {
	return func() {
		m.mu.Lock()
		delete(m.partitionFailovers, p)
		m.mu.Unlock()
	}
}

func (m *metadataAPI) newPartitionFailoverHandler(p *partition) failoverHandler {
	return func(ctx context.Context) *status.Status {
		return m.electNewPartitionLeader(ctx, p)
	}
}

// SetStreamReadonly sets a stream's readonly flag if this server is the
// metadata leader. If it is not, it will forward the request to the leader and
// return the response. This operation is replicated by Raft. If successful,
// this will return once the readonly flag has been set.
func (m *metadataAPI) SetStreamReadonly(ctx context.Context, req *proto.SetStreamReadonlyOp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		isLeader, st := m.propagateSetStreamReadonly(ctx, req)
		if st != nil {
			return st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return nil
		}
	}

	// Replicate the stream readonly flag through Raft.
	op := &proto.RaftLog{
		Op:                  proto.Op_SET_STREAM_READONLY,
		SetStreamReadonlyOp: req,
	}

	// Wait on result of setting the readonly flag.
	future, err := m.getRaft().applyOperation(ctx, op, m.checkSetStreamReadonlyPreconditions)
	if err != nil {
		code := codes.FailedPrecondition
		if err == ErrStreamNotFound || err == ErrPartitionNotFound {
			code = codes.NotFound
		}
		return status.Newf(code, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to set stream readonly flag: %v", err.Error())
	}

	return nil
}

// JoinConsumerGroup adds a consumer to a consumer group if this server is the
// metadata leader. The group is created first if it does not yet exist. If
// this server is not the metadata leader, it will forward the request to the
// leader and return the response. This operation is replicated by Raft. If
// successful, this will return once the consumer has been added to the group.
// Returns the group coordinator ID and coordinator epoch on success.
func (m *metadataAPI) JoinConsumerGroup(ctx context.Context, req *proto.JoinConsumerGroupOp) (
	string, uint64, *status.Status) {

	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		resp, isLeader, st := m.propagateJoinConsumerGroup(ctx, req)
		if st != nil {
			return "", 0, st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return resp.Coordinator, resp.Epoch, nil
		}
	}

	// Check if group exists. If it doesn't, create it with the member.
	m.consumerGroupsMu.RLock()
	group, ok := m.consumerGroups[req.GroupId]
	m.consumerGroupsMu.RUnlock()
	if !ok {
		coordinator, err := m.createConsumerGroup(ctx, req)
		if err != nil {
			return "", 0, status.New(codes.FailedPrecondition, err.Error())
		}
		// The coordinator epoch for a new group is always 0.
		return coordinator, 0, nil
	}

	// If the group already existed, replicate the join request through Raft.
	op := &proto.RaftLog{
		Op:                  proto.Op_JOIN_CONSUMER_GROUP,
		JoinConsumerGroupOp: req,
	}

	// Wait on result of replication.
	future, err := m.getRaft().applyOperation(ctx, op, m.checkJoinConsumerGroupPreconditions)
	if err != nil {
		code := codes.FailedPrecondition
		if err == ErrConsumerGroupNotFound {
			code = codes.NotFound
		}
		return "", 0, status.New(code, err.Error())
	}
	if err := future.Error(); err != nil {
		return "", 0, status.Newf(codes.Internal, "Failed to join consumer group: %v", err.Error())
	}

	coordinator, epoch := group.GetCoordinator()
	return coordinator, epoch, nil
}

// LeaveConsumerGroup removes a consumer from a consumer group. If this is the
// last member of the group, the group will be deleted. This operation is
// replicated by Raft. If successful, this will return once the consumer has
// been removed from the group.
func (m *metadataAPI) LeaveConsumerGroup(ctx context.Context, req *proto.LeaveConsumerGroupOp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		isLeader, st := m.propagateLeaveConsumerGroup(ctx, req)
		if st != nil {
			return st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return nil
		}
	}

	// Replicate the leave request through Raft.
	op := &proto.RaftLog{
		Op:                   proto.Op_LEAVE_CONSUMER_GROUP,
		LeaveConsumerGroupOp: req,
	}

	// Wait on result of replication.
	future, err := m.getRaft().applyOperation(ctx, op, m.checkLeaveConsumerGroupPreconditions)
	if err != nil {
		code := codes.FailedPrecondition
		if err == ErrConsumerGroupNotFound {
			code = codes.NotFound
		}
		return status.New(code, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to set stream readonly flag: %v", err.Error())
	}

	return nil
}

// ReportGroupCoordinator marks the consumer group coordinator as unresponsive
// with respect to the specified member if this server is the metadata leader.
// If it is not, it will forward the request to the leader and return the
// response. If a quorum of members report the coordinator within a bounded
// period, the metadata leader will select a new group coordinator.
func (m *metadataAPI) ReportGroupCoordinator(ctx context.Context, req *proto.ReportConsumerGroupCoordinatorOp) *status.Status {
	// Forward the request if we're not the leader.
	if !m.IsLeader() {
		isLeader, st := m.propagateReportGroupCoordinator(ctx, req)
		if st != nil {
			return st
		}
		// If we have since become leader, continue on with the request.
		if !isLeader {
			return nil
		}
	}

	// Verify the group exists.
	group := m.GetConsumerGroup(req.GroupId)
	if group == nil {
		return status.New(codes.FailedPrecondition, fmt.Sprintf("No such consumer group %s", req.GroupId))
	}

	// Check the group epoch.
	coordinator, epoch := group.GetCoordinator()
	if req.Coordinator != coordinator || req.Epoch != epoch {
		return status.New(
			codes.FailedPrecondition,
			fmt.Sprintf("Coordinator generation mismatch, current coordinator: %s epoch: %d, got coordinator: %s epoch: %d",
				coordinator, epoch, req.Coordinator, req.Epoch))
	}

	// Ensure the consumer is actually a member.
	if !group.IsMember(req.ConsumerId) {
		return status.New(
			codes.FailedPrecondition,
			fmt.Sprintf("Consumer %s is not a member of consumer group %s", req.ConsumerId, req.GroupId))
	}

	m.consumerGroupsMu.Lock()
	failover := m.groupFailovers[group]
	if failover == nil {
		failover = newGroupFailoverStatus(
			group,
			m.config.Groups.CoordinatorTimeout,
			m.newGroupFailoverExpiredHandler(group),
			m.newGroupFailoverHandler(group),
		)
		m.groupFailovers[group] = failover
	}
	m.consumerGroupsMu.Unlock()

	return failover.report(ctx, req.ConsumerId)
}

func (m *metadataAPI) newGroupFailoverExpiredHandler(g *consumerGroup) failoverExpiredHandler {
	return func() {
		m.consumerGroupsMu.Lock()
		delete(m.groupFailovers, g)
		m.consumerGroupsMu.Unlock()
	}
}

func (m *metadataAPI) newGroupFailoverHandler(g *consumerGroup) failoverHandler {
	return func(ctx context.Context) *status.Status {
		return m.electNewGroupCoordinator(ctx, g)
	}
}

// createConsumerGroup creates a new consumer group bootstrapped with the
// provided consumer by replicating the operation via Raft. This should be
// called after checking that this server is the metadata leader, but if the
// server is not the leader or the leadership has since changed, the operation
// will return an error. This will also return an error if the group being
// created already exists. This returns the group coordinator on success.
func (m *metadataAPI) createConsumerGroup(ctx context.Context, req *proto.JoinConsumerGroupOp) (string, error) {
	brokers, err := m.getClusterServerIDs()
	if err != nil {
		return "", errors.Wrap(err, "failed to select group coordinator")
	}
	coordinator := m.selectGroupCoordinator(brokers)

	// Replicate the group create through Raft.
	op := &proto.RaftLog{
		Op: proto.Op_CREATE_CONSUMER_GROUP,
		CreateConsumerGroupOp: &proto.CreateConsumerGroupOp{
			ConsumerGroup: &proto.ConsumerGroup{
				Id:          req.GroupId,
				Coordinator: coordinator,
				Members:     []*proto.Consumer{{Id: req.ConsumerId, Streams: req.Streams}},
			},
		},
	}

	// Wait on result of replication.
	future, err := m.getRaft().applyOperation(ctx, op, m.checkCreateConsumerGroupPreconditions)
	if err != nil {
		return "", err
	}
	if err := future.Error(); err != nil {
		return "", err
	}

	return coordinator, nil
}

// AddConsumerGroup adds the given consumer group to the metadata store. It
// returns an error if a consumer group with the same ID already exists.
func (m *metadataAPI) AddConsumerGroup(protoGroup *proto.ConsumerGroup, recovered bool) (*consumerGroup, error) {
	m.consumerGroupsMu.Lock()
	defer m.consumerGroupsMu.Unlock()

	if _, ok := m.consumerGroups[protoGroup.Id]; ok {
		return nil, ErrConsumerGroupExists
	}

	group := newConsumerGroup(m.config.Clustering.ServerID, m.config.Groups.ConsumerTimeout,
		protoGroup, recovered, m.logger, m.removeConsumerGroupMember, m.countStreamPartitions)
	m.consumerGroups[protoGroup.Id] = group
	coordinator, _ := group.GetCoordinator()

	// Update broker load counts.
	m.stats.Lock()
	m.stats.brokerCoordinatorLoad[coordinator]++
	m.stats.Unlock()

	return group, nil
}

// removeConsumerGroupMember sends a LeaveConsumerGroup request to the
// controller to remove the expired consumer from the group.
func (m *metadataAPI) removeConsumerGroupMember(groupID, consumerID string) error {
	req := &proto.LeaveConsumerGroupOp{
		GroupId:    groupID,
		ConsumerId: consumerID,
		Expired:    true,
	}
	if err := m.LeaveConsumerGroup(context.Background(), req); err != nil {
		return err.Err()
	}
	return nil
}

// countStreamPartitions returns the number of partitions for the stream or 0
// if the stream does not exist.
func (m *metadataAPI) countStreamPartitions(streamName string) int32 {
	stream := m.GetStream(streamName)
	if stream == nil {
		return 0
	}
	return int32(len(stream.GetPartitions()))
}

// removeConsumerGroup removes the given consumer group from the metadata
// store. This should only be called within the scope of the consumerGroupsMu.
func (m *metadataAPI) removeConsumerGroup(groupID string) {
	group := m.consumerGroups[groupID]
	if group == nil {
		return
	}
	group.Close()
	delete(m.consumerGroups, groupID)
	coordinator, _ := group.GetCoordinator()

	// Update broker load counts.
	m.stats.Lock()
	if m.stats.brokerCoordinatorLoad[coordinator] > 0 {
		m.stats.brokerCoordinatorLoad[coordinator]--
	}
	m.stats.Unlock()
}

// AddConsumerToGroup adds the given consumer to the consumer group. It returns
// an error if the group does not exist, the consumer is already a member of
// the group, or any of the provided streams do not exist.
func (m *metadataAPI) AddConsumerToGroup(groupID, consumerID string, streams []string, epoch uint64) error {
	m.consumerGroupsMu.RLock()
	group := m.consumerGroups[groupID]
	m.consumerGroupsMu.RUnlock()

	if group == nil {
		return ErrConsumerGroupNotFound
	}

	return group.AddMember(consumerID, streams, epoch)
}

// RemoveConsumerFromGroup removes the given consumer from the consumer group.
// It returns an error if the group does not exist or the consumer is not a
// member of the group. If this is the last member of the group, the group will
// be deleted. Returns a bool indicating if this was the last member of the
// group and the group has been deleted.
func (m *metadataAPI) RemoveConsumerFromGroup(groupID, consumerID string, epoch uint64) (bool, error) {
	m.consumerGroupsMu.Lock()
	defer m.consumerGroupsMu.Unlock()
	group := m.consumerGroups[groupID]

	if group == nil {
		return false, ErrConsumerGroupNotFound
	}

	lastMember, err := group.RemoveMember(consumerID, epoch)
	if err != nil {
		return false, err
	}

	// If the last member was removed, delete the group.
	if lastMember {
		m.removeConsumerGroup(groupID)
	}
	return lastMember, nil
}

// GetConsumerGroupAssignments returns the group's partition assignments for
// the given consumer and the group epoch.
func (m *metadataAPI) GetConsumerGroupAssignments(groupID, consumerID string, epoch uint64) (
	partitionAssignments, uint64, error) {

	m.consumerGroupsMu.RLock()
	group := m.consumerGroups[groupID]
	m.consumerGroupsMu.RUnlock()

	if group == nil {
		return nil, 0, ErrConsumerGroupNotFound
	}

	return group.GetAssignments(consumerID, epoch)
}

// AddStream adds the given stream and its partitions to the metadata store. It
// returns an error if a stream with the same name or any partitions with the
// same ID for the stream already exist. If the stream is recovered, this will
// not start the partitions until recovery completes. Partitions will also not
// be started if they are currently paused.
func (m *metadataAPI) AddStream(protoStream *proto.Stream, recovered bool, epoch uint64) (*stream, error) {
	if len(protoStream.Partitions) == 0 {
		return nil, errors.New("stream has no partitions")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.streams[protoStream.Name]
	if ok {
		if !recovered {
			return nil, ErrStreamExists
		}
		// If this operation is being applied during recovery, check if this
		// stream is tombstoned, i.e. was marked for deletion previously. If it
		// is, un-tombstone it by closing the existing stream and then
		// recreating it, leaving the existing data intact.
		if !existing.IsTombstoned() {
			// This is an invalid state because it means the stream already
			// exists.
			return nil, ErrStreamExists
		}
		// Un-tombstone by closing the existing stream and removing it from
		// the streams store so that it can be recreated.
		if err := existing.Close(); err != nil {
			return nil, err
		}
		m.removeStream(existing, epoch)
	}

	config := protoStream.GetConfig()
	creationTime := time.Unix(0, protoStream.CreationTimestamp)
	stream := newStream(protoStream.Name, protoStream.Subject, config, creationTime, m.config)
	m.streams[protoStream.Name] = stream

	for _, partition := range protoStream.Partitions {
		if err := m.addPartition(stream, partition, recovered, config); err != nil {
			m.removeStream(stream, epoch)
			return nil, err
		}
	}

	// Update broker load counts.
	m.stats.Lock()
	for _, partition := range stream.GetPartitions() {
		for _, broker := range partition.Replicas {
			m.stats.brokerPartitionLoad[broker]++
		}
		m.stats.brokerLeaderLoad[partition.Leader]++
	}
	m.stats.Unlock()

	return stream, nil
}

func (m *metadataAPI) addPartition(stream *stream, protoPartition *proto.Partition, recovered bool, config *proto.StreamConfig) error {
	if p := stream.GetPartition(protoPartition.Id); p != nil {
		// Partition already exists for stream.
		return fmt.Errorf("partition %d already exists for stream %s",
			protoPartition.Id, protoPartition.Stream)
	}

	// This will initialize/recover the durable commit log.
	partition, err := m.newPartition(protoPartition, recovered, config)
	if err != nil {
		return err
	}
	stream.SetPartition(protoPartition.Id, partition)

	// If we're loading a partition that was paused, we need to re-pause it.
	if protoPartition.Paused {
		if err := partition.Pause(); err != nil {
			return err
		}
	}

	// Start leader/follower loop if necessary.
	leader, epoch := partition.GetLeader()
	return partition.SetLeader(leader, epoch)
}

// ResumePartition unpauses the given stream partition in the metadata store.
// It returns ErrPartitionNotFound if there is no partition with the ID for the
// stream. If the partition is recovered, this will not start the partition
// until recovery completes.
func (m *metadataAPI) ResumePartition(streamName string, id int32, recovered bool) (*partition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.streams[streamName]
	if !ok {
		return nil, ErrStreamNotFound
	}
	partition := stream.GetPartition(id)
	if partition == nil {
		return nil, ErrPartitionNotFound
	}

	// If it's not paused, do nothing.
	if !partition.IsPaused() {
		return partition, nil
	}

	// Resume the partition by replacing it.
	partition, err := m.replacePartition(partition, recovered, stream.GetConfig())
	if err != nil {
		return nil, err
	}
	// Update latest pause status change timestamp.
	partition.pauseTimestamps.update()

	stream.SetPartition(id, partition)

	// Start leader/follower loop if necessary.
	leader, epoch := partition.GetLeader()
	err = partition.SetLeader(leader, epoch)

	// Update broker load counts.
	m.stats.Lock()
	for _, broker := range partition.GetReplicas() {
		m.stats.brokerPartitionLoad[broker]++
	}
	m.stats.brokerLeaderLoad[leader]++
	m.stats.Unlock()

	return partition, err
}

// RemoveFromISR removes the given replica from the partition's ISR if the
// given epoch is greater than the current epoch.
func (m *metadataAPI) RemoveFromISR(streamName, replica string, partitionID int32, epoch uint64) error {
	partition := m.GetPartition(streamName, partitionID)
	if partition == nil {
		return fmt.Errorf("No such partition [stream=%s, partition=%d]", streamName, partitionID)
	}

	// Idempotency check.
	if partition.GetEpoch() >= epoch {
		return nil
	}

	if err := partition.RemoveFromISR(replica); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to remove %s from ISR for partition %s",
			replica, partition))
	}

	partition.SetEpoch(epoch)
	return nil
}

// AddToISR adds the given replica to the partition's ISR if the given epoch is
// greater than the current epoch.
func (m *metadataAPI) AddToISR(streamName, replica string, partitionID int32, epoch uint64) error {
	partition := m.GetPartition(streamName, partitionID)
	if partition == nil {
		return fmt.Errorf("No such partition [stream=%s, partition=%d]", streamName, partitionID)
	}

	// Idempotency check.
	if partition.GetEpoch() >= epoch {
		return nil
	}

	if err := partition.AddToISR(replica); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to add %s to ISR for partition %s",
			replica, partition))
	}

	partition.SetEpoch(epoch)
	return nil
}

// ChangeLeader changes the partition's leader to the given replica if the
// given epoch is greater than the current epoch.
func (m *metadataAPI) ChangeLeader(streamName, leader string, partitionID int32, epoch uint64) error {
	partition := m.GetPartition(streamName, partitionID)
	if partition == nil {
		return fmt.Errorf("No such partition [stream=%s, partition=%d]", streamName, partitionID)
	}

	// Idempotency check.
	if partition.GetEpoch() >= epoch {
		return nil
	}

	oldLeader, _ := partition.GetLeader()

	if err := partition.SetLeader(leader, epoch); err != nil {
		return errors.Wrap(err, "failed to change partition leader")
	}

	partition.SetEpoch(epoch)

	// Update broker load counts.
	m.stats.Lock()
	if m.stats.brokerLeaderLoad[oldLeader] > 0 {
		m.stats.brokerLeaderLoad[oldLeader]--
	}
	m.stats.brokerLeaderLoad[leader]++
	m.stats.Unlock()

	return nil
}

// ChangeGroupCoordinator changes the consumer group's coordinator to the given
// broker if the given epoch is greater than the current epoch.
func (m *metadataAPI) ChangeGroupCoordinator(groupID, coordinator string, newEpoch uint64) error {
	group := m.GetConsumerGroup(groupID)
	if group == nil {
		return fmt.Errorf("No such consumer group %s", groupID)
	}

	// Idempotency check.
	oldCoordinator, epoch := group.GetCoordinator()
	if epoch >= newEpoch {
		return nil
	}

	if err := group.SetCoordinator(coordinator, newEpoch); err != nil {
		return errors.Wrap(err, "failed to change group coordinator")
	}

	// Update broker load counts.
	m.stats.Lock()
	if m.stats.brokerCoordinatorLoad[oldCoordinator] > 0 {
		m.stats.brokerCoordinatorLoad[oldCoordinator]--
	}
	m.stats.brokerCoordinatorLoad[coordinator]++
	m.stats.Unlock()

	return nil
}

// PausePartitions pauses the given partitions for the stream. If the list of
// partitions is empty, this pauses all partitions.
func (m *metadataAPI) PausePartitions(streamName string, partitions []int32, resumeAll bool) error {
	stream := m.GetStream(streamName)
	if stream == nil {
		return ErrStreamNotFound
	}

	paused, err := stream.Pause(partitions, resumeAll)
	if err != nil {
		return errors.Wrap(err, "failed to pause stream")
	}

	// Update broker load counts.
	m.stats.Lock()
	for _, partition := range paused {
		for _, broker := range partition.Replicas {
			if m.stats.brokerPartitionLoad[broker] > 0 {
				m.stats.brokerPartitionLoad[broker]--
			}
		}
		if m.stats.brokerLeaderLoad[partition.Leader] > 0 {
			m.stats.brokerLeaderLoad[partition.Leader]--
		}
	}
	m.stats.Unlock()

	return nil
}

// SetReadonly changes the stream partitions' readonly flag in the metadata
// store.
func (m *metadataAPI) SetReadonly(streamName string, partitions []int32, readonly bool) error {
	stream := m.GetStream(streamName)
	if stream == nil {
		return ErrStreamNotFound
	}

	err := stream.SetReadonly(partitions, readonly)
	if err != nil {
		return errors.Wrap(err, "failed to set stream as readonly")
	}

	return nil
}

// GetStreams returns all streams from the metadata store.
func (m *metadataAPI) GetStreams() []*stream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getStreams()
}

// GetStream returns the stream with the given name or nil if no such stream
// exists.
func (m *metadataAPI) GetStream(name string) *stream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.streams[name]
}

// GetPartition returns the stream partition for the given stream and partition
// ID. It returns nil if no such partition exists.
func (m *metadataAPI) GetPartition(streamName string, id int32) *partition {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stream, ok := m.streams[streamName]
	if !ok {
		return nil
	}
	return stream.GetPartition(id)
}

// GetConsumerGroups returns all consumer groups from the metadata store.
func (m *metadataAPI) GetConsumerGroups() []*consumerGroup {
	m.consumerGroupsMu.RLock()
	defer m.consumerGroupsMu.RUnlock()
	return m.getConsumerGroups()
}

func (m *metadataAPI) getConsumerGroups() []*consumerGroup {
	groups := make([]*consumerGroup, 0, len(m.consumerGroups))
	for _, group := range m.consumerGroups {
		groups = append(groups, group)
	}
	return groups
}

// GetConsumerGroup returns the consumer group with the given id. It returns
// nil if no such group exists.
func (m *metadataAPI) GetConsumerGroup(id string) *consumerGroup {
	m.consumerGroupsMu.RLock()
	defer m.consumerGroupsMu.RUnlock()
	return m.consumerGroups[id]
}

// Reset closes all streams and consumer groups and clears all existing state
// in the metadata store.
func (m *metadataAPI) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, stream := range m.getStreams() {
		if err := stream.Close(); err != nil {
			return err
		}
	}
	m.streams = make(map[string]*stream)
	m.consumerGroupsMu.Lock()
	defer m.consumerGroupsMu.Unlock()
	for _, group := range m.getConsumerGroups() {
		group.Close()
	}
	m.consumerGroups = make(map[string]*consumerGroup)
	m.resetFailovers()
	return nil
}

// resetFailovers cancels all in-flight failovers and clears the failover state
// in the metadata store. Both the metadata API and consumer groups mutexes
// must be held when calling this.
func (m *metadataAPI) resetFailovers() {
	for _, failover := range m.partitionFailovers {
		failover.cancel()
	}
	m.partitionFailovers = make(map[*partition]*failoverStatus)

	for _, failover := range m.groupFailovers {
		failover.cancel()
	}
	m.groupFailovers = make(map[*consumerGroup]*failoverStatus)
}

// RemoveStream closes the stream, removes it from the metadata store, and
// deletes the associated on-disk data for it. However, if this operation is
// being applied during Raft recovery, this will only mark the stream with a
// tombstone. Tombstoned streams will be deleted after the recovery process
// completes.
func (m *metadataAPI) RemoveStream(stream *stream, recovered bool, epoch uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If this operation is being applied during recovery, only tombstone the
	// stream. We don't want to delete streams until recovery finishes to avoid
	// deleting potentially valid data, e.g. in the case of a stream being
	// deleted, recreated, and then published to. In this scenario, the
	// recreate will un-tombstone the stream.
	if recovered {
		stream.Tombstone()
	} else {
		if err := m.deleteStream(stream, epoch); err != nil {
			return err
		}
	}

	// Update broker load counts.
	m.stats.Lock()
	for _, partition := range stream.GetPartitions() {
		for _, broker := range partition.Replicas {
			if m.stats.brokerPartitionLoad[broker] > 0 {
				m.stats.brokerPartitionLoad[broker]--
			}
		}
		if m.stats.brokerLeaderLoad[partition.Leader] > 0 {
			m.stats.brokerLeaderLoad[partition.Leader]--
		}
	}
	m.stats.Unlock()

	return nil
}

// RemoveTombstonedStream closes the tombstoned stream, removes it from the
// metadata store, and deletes the associated on-disk data for it.
func (m *metadataAPI) RemoveTombstonedStream(stream *stream, epoch uint64) error {
	if !stream.IsTombstoned() {
		return fmt.Errorf("cannot delete stream %s because it is not tombstoned", stream)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deleteStream(stream, epoch)
}

// LostLeadership should be called when the server loses metadata leadership.
// This will cancel in-flight failovers.
func (m *metadataAPI) LostLeadership() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.consumerGroupsMu.Lock()
	defer m.consumerGroupsMu.Unlock()
	m.resetFailovers()
}

// deleteStream deletes the stream and the associated on-disk data for it.
func (m *metadataAPI) deleteStream(stream *stream, epoch uint64) error {
	err := stream.Delete()
	if err != nil {
		return errors.Wrap(err, "failed to delete stream")
	}

	// Remove the stream data directory
	streamDataDir := filepath.Join(m.Server.config.DataDir, "streams", stream.GetName())
	err = os.RemoveAll(streamDataDir)
	if err != nil {
		return errors.Wrap(err, "failed to delete stream data directory")
	}

	m.removeStream(stream, epoch)
	return nil
}

// removeStream removes the stream from the stream store, cancels any
// in-flight failovers for its partitions, and triggers a rebalance of consumer
// group assignments.
func (m *metadataAPI) removeStream(stream *stream, epoch uint64) {
	delete(m.streams, stream.GetName())
	for _, partition := range stream.GetPartitions() {
		failover, ok := m.partitionFailovers[partition]
		if ok {
			failover.cancel()
			delete(m.partitionFailovers, partition)
		}
	}
	m.startGoroutine(func() {
		m.consumerGroupsMu.RLock()
		for _, group := range m.consumerGroups {
			group.StreamDeleted(stream.GetName(), epoch)
		}
		m.consumerGroupsMu.RUnlock()
	})
}

func (m *metadataAPI) getStreams() []*stream {
	streams := make([]*stream, 0, len(m.streams))
	for _, stream := range m.streams {
		streams = append(streams, stream)
	}
	return streams
}

// getPartitionReplicas selects replicationFactor replicas to participate in
// the stream partition. Replicas are selected based on the amount of partition
// load they have.
func (m *metadataAPI) getPartitionReplicas(replicationFactor int32) ([]string, *status.Status) {
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

	// Order servers by partition load.
	m.stats.RLock()
	sort.SliceStable(ids, func(i, j int) bool {
		return m.stats.brokerPartitionLoad[ids[i]] < m.stats.brokerPartitionLoad[ids[j]]
	})
	m.stats.RUnlock()

	return ids[:replicationFactor], nil
}

// getClusterServerIDs returns a list of all the broker IDs in the cluster.
func (m *metadataAPI) getClusterServerIDs() ([]string, error) {
	future := m.getRaft().GetConfiguration()
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

// electNewPartitionLeader selects a new leader for the given partition,
// applies this update to the Raft group, and notifies the replica set. This
// will fail if the current broker is not the metadata leader.
func (m *metadataAPI) electNewPartitionLeader(ctx context.Context, partition *partition) *status.Status {
	isr := partition.GetISR()
	// TODO: add support for "unclean" leader elections.
	if len(isr) <= 1 {
		return status.New(codes.FailedPrecondition, "No ISR candidates")
	}
	var (
		candidates = make([]string, 0, len(isr)-1)
		leader, _  = partition.GetLeader()
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

	// Select a new leader.
	leader = m.selectPartitionLeader(candidates)

	// Replicate leader change through Raft.
	op := &proto.RaftLog{
		Op: proto.Op_CHANGE_LEADER,
		ChangeLeaderOp: &proto.ChangeLeaderOp{
			Stream:    partition.Stream,
			Partition: partition.Id,
			Leader:    leader,
		},
	}

	// Wait on result of replication.
	future, err := m.getRaft().applyOperation(ctx, op, m.checkChangeLeaderPreconditions)
	if err != nil {
		return status.Newf(codes.FailedPrecondition, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to replicate leader change: %v", err.Error())
	}

	return nil
}

// electNewGroupCoordinator selects a new coordinator for the given consumer
// group and applies this update to the Raft group. This will fail if the
// current broker is not the metadata leader.
func (m *metadataAPI) electNewGroupCoordinator(ctx context.Context, group *consumerGroup) *status.Status {
	brokers, err := m.getClusterServerIDs()
	if err != nil {
		return status.New(codes.Internal, err.Error())
	}
	var (
		candidates        = make([]string, 0, len(brokers)-1)
		oldCoordinator, _ = group.GetCoordinator()
	)
	for _, candidate := range brokers {
		if candidate == oldCoordinator {
			continue
		}
		candidates = append(candidates, candidate)
	}
	if len(candidates) == 0 {
		return status.New(codes.FailedPrecondition, "No group coordinator candidates")
	}

	// Select a new coordinator.
	coordinator := m.selectGroupCoordinator(candidates)

	// Replicate coordinator change through Raft.
	op := &proto.RaftLog{
		Op: proto.Op_CHANGE_CONSUMER_GROUP_COORDINATOR,
		ChangeConsumerGroupCoordinatorOp: &proto.ChangeConsumerGroupCoordinatorOp{
			GroupId:     group.GetID(),
			Coordinator: coordinator,
		},
	}

	// Wait on result of replication.
	future, err := m.getRaft().applyOperation(ctx, op, m.checkChangeGroupCoordinatorPreconditions)
	if err != nil {
		return status.New(codes.FailedPrecondition, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to replicate coordinator change: %v", err.Error())
	}

	return nil
}

// propagateCreateStream forwards a CreateStream request to the metadata
// leader. The bool indicates if this server has since become leader and the
// request should be performed locally. A Status is returned if the propagated
// request failed.
func (m *metadataAPI) propagateCreateStream(ctx context.Context, req *proto.CreateStreamOp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:             proto.Op_CREATE_STREAM,
		CreateStreamOp: req,
	}
	_, isLeader, status := m.propagateRequest(ctx, propagate)
	return isLeader, status
}

// propagateDeleteStream forwards a DeleteStream request to the metadata
// leader. The bool indicates if this server has since become leader and the
// request should be performed locally. A Status is returned if the propagated
// request failed.
func (m *metadataAPI) propagateDeleteStream(ctx context.Context, req *proto.DeleteStreamOp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:             proto.Op_DELETE_STREAM,
		DeleteStreamOp: req,
	}
	_, isLeader, status := m.propagateRequest(ctx, propagate)
	return isLeader, status
}

// propagatePauseStream forwards a PauseStream request to the metadata
// leader. The bool indicates if this server has since become leader and the
// request should be performed locally. A Status is returned if the propagated
// request failed.
func (m *metadataAPI) propagatePauseStream(ctx context.Context, req *proto.PauseStreamOp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:            proto.Op_PAUSE_STREAM,
		PauseStreamOp: req,
	}
	_, isLeader, status := m.propagateRequest(ctx, propagate)
	return isLeader, status
}

// propagateResumeStream forwards a ResumeStream request to the metadata
// leader. The bool indicates if this server has since become leader and the
// request should be performed locally. A Status is returned if the propagated
// request failed.
func (m *metadataAPI) propagateResumeStream(ctx context.Context, req *proto.ResumeStreamOp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:             proto.Op_RESUME_STREAM,
		ResumeStreamOp: req,
	}
	_, isLeader, status := m.propagateRequest(ctx, propagate)
	return isLeader, status
}

// propagateShrinkISR forwards a ShrinkISR request to the metadata leader. The
// bool indicates if this server has since become leader and the request should
// be performed locally. A Status is returned if the propagated request failed.
func (m *metadataAPI) propagateShrinkISR(ctx context.Context, req *proto.ShrinkISROp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:          proto.Op_SHRINK_ISR,
		ShrinkISROp: req,
	}
	_, isLeader, status := m.propagateRequest(ctx, propagate)
	return isLeader, status
}

// propagateExpandISR forwards a ExpandISR request to the metadata leader. The
// bool indicates if this server has since become leader and the request should
// be performed locally. A Status is returned if the propagated request failed.
func (m *metadataAPI) propagateExpandISR(ctx context.Context, req *proto.ExpandISROp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:          proto.Op_EXPAND_ISR,
		ExpandISROp: req,
	}
	_, isLeader, status := m.propagateRequest(ctx, propagate)
	return isLeader, status
}

// propagateReportLeader forwards a ReportLeader request to the metadata
// leader. The bool indicates if this server has since become leader and the
// request should be performed locally. A Status is returned if the propagated
// request failed.
func (m *metadataAPI) propagateReportLeader(ctx context.Context, req *proto.ReportLeaderOp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:             proto.Op_REPORT_LEADER,
		ReportLeaderOp: req,
	}
	_, isLeader, status := m.propagateRequest(ctx, propagate)
	return isLeader, status
}

// propagateSetStreamReadonly forwards a SetStreamReadonly request to the
// metadata leader. The bool indicates if this server has since become leader
// and the request should be performed locally. A Status is returned if the
// propagated request failed.
func (m *metadataAPI) propagateSetStreamReadonly(ctx context.Context, req *proto.SetStreamReadonlyOp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:                  proto.Op_SET_STREAM_READONLY,
		SetStreamReadonlyOp: req,
	}
	_, isLeader, status := m.propagateRequest(ctx, propagate)
	return isLeader, status
}

// propagateJoinConsumerGroup forwards a JoinConsumerGroup request to the
// metadata leader. The bool indicates if this server has since become leader
// and the request should be performed locally. A Status is returned if the
// propagated request failed.
func (m *metadataAPI) propagateJoinConsumerGroup(ctx context.Context, req *proto.JoinConsumerGroupOp) (
	*proto.PropagatedResponse_JoinConsumerGroupResponse, bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:                  proto.Op_JOIN_CONSUMER_GROUP,
		JoinConsumerGroupOp: req,
	}
	resp, isLeader, status := m.propagateRequest(ctx, propagate)
	if status != nil {
		return nil, false, status
	}
	if isLeader {
		return nil, true, nil
	}
	return resp.JoinConsumerGroupResp, isLeader, status
}

// propagateLeaveConsumerGroup forwards a LeaveConsumerGroup request to the
// metadata leader. The bool indicates if this server has since become leader
// and the request should be performed locally. A Status is returned if the
// propagated request failed.
func (m *metadataAPI) propagateLeaveConsumerGroup(ctx context.Context, req *proto.LeaveConsumerGroupOp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:                   proto.Op_LEAVE_CONSUMER_GROUP,
		LeaveConsumerGroupOp: req,
	}
	_, isLeader, status := m.propagateRequest(ctx, propagate)
	return isLeader, status
}

// propagateReportGroupCoordinator forwards a ReportGroupCoordinator request to
// the metadata leader. The bool indicates if this server has since become
// leader and the request should be performed locally. A Status is returned if
// the propagated request failed.
func (m *metadataAPI) propagateReportGroupCoordinator(ctx context.Context, req *proto.ReportConsumerGroupCoordinatorOp) (
	bool, *status.Status) {

	propagate := &proto.PropagatedRequest{
		Op:                               proto.Op_REPORT_CONSUMER_GROUP_COORDINATOR,
		ReportConsumerGroupCoordinatorOp: req,
	}
	_, isLeader, status := m.propagateRequest(ctx, propagate)
	return isLeader, status
}

// propagateRequest forwards a metadata request to the metadata leader. The
// bool indicates if this server has since become leader and the request should
// be performed locally. A Status is returned if the propagated request failed.
func (m *metadataAPI) propagateRequest(ctx context.Context, req *proto.PropagatedRequest) (*proto.PropagatedResponse, bool, *status.Status) {
	// Check if there is currently a metadata leader.
	isLeader, err := m.waitForMetadataLeader(ctx)
	if err != nil {
		return nil, false, status.New(codes.Internal, err.Error())
	}
	// This server has since become metadata leader, so the request should be
	// performed locally.
	if isLeader {
		return nil, true, nil
	}

	data, err := proto.MarshalPropagatedRequest(req)
	if err != nil {
		panic(err)
	}

	ctx, cancel := ensureTimeout(ctx, defaultPropagateTimeout)
	defer cancel()

	resp, err := m.nc.RequestWithContext(ctx, m.getPropagateInbox(), data)
	if err != nil {
		return nil, false, status.New(codes.Internal, err.Error())
	}

	r, err := proto.UnmarshalPropagatedResponse(resp.Data)
	if err != nil {
		m.logger.Errorf("metadata: Invalid response for propagated request: %v", err)
		return nil, false, status.New(codes.Internal, "invalid response")
	}
	if r.Error != nil {
		return nil, false, status.New(codes.Code(r.Error.Code), r.Error.Msg)
	}

	return r, false, nil
}

// waitForMetadataLeader waits up to the deadline specified on the Context
// until a metadata leader is established. If no leader is established in time,
// an error is returned. The bool indicates if this server has become the
// leader. False and a nil error indicates another server has become leader.
func (m *metadataAPI) waitForMetadataLeader(ctx context.Context) (bool, error) {
	for {
		if leader := m.getRaft().Leader(); leader != "" {
			if string(leader) == m.config.Clustering.ServerID {
				return true, nil
			}
			break
		}
		// Wait up to deadline for a metadata leader to be established.
		deadline, _ := ctx.Deadline()
		if time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		return false, errors.New("no known metadata leader")
	}
	return false, nil
}

// waitForPartitionLeader does a best-effort wait for the leader of the given
// partition to create and start the partition.
func (m *metadataAPI) waitForPartitionLeader(ctx context.Context, partition *proto.Partition) {
	ctx, cancel := ensureTimeout(ctx, defaultPropagateTimeout)
	defer cancel()

	if partition.Leader == m.config.Clustering.ServerID {
		// If we're the partition leader, there's no need to make a status
		// request. We can just apply a Raft barrier since the FSM is local.
		deadline, _ := ctx.Deadline()
		if err := m.getRaft().Barrier(time.Until(deadline)).Error(); err != nil {
			m.logger.Warnf("Failed to apply Raft barrier: %v", err)
		}
		return
	}

	req, err := proto.MarshalPartitionStatusRequest(&proto.PartitionStatusRequest{
		Stream:    partition.Stream,
		Partition: partition.Id,
	})
	if err != nil {
		panic(err)
	}

	inbox := m.getPartitionStatusInbox(partition.Leader)
	for i := 0; i < 5; i++ {
		resp, err := m.ncRaft.RequestWithContext(ctx, inbox, req)
		if err != nil {
			m.logger.Warnf(
				"Failed to get status for partition %s from leader %s: %v",
				partition, partition.Leader, err)
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-ctx.Done():
				return
			}
		}
		statusResp, err := proto.UnmarshalPartitionStatusResponse(resp.Data)
		if err != nil {
			m.logger.Warnf(
				"Invalid status response for partition %s from leader %s: %v",
				partition, partition.Leader, err)
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-ctx.Done():
				return
			}
		}
		if !statusResp.Exists || !statusResp.IsLeader {
			// The leader hasn't finished creating the partition, so wait a bit
			// and retry.
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-ctx.Done():
				return
			}
		}
		break
	}
}

// checkCreateStreamPreconditions checks if the stream to be created already
// exists. If it does, it returns ErrStreamExists. Otherwise, it returns nil.
func (m *metadataAPI) checkCreateStreamPreconditions(op *proto.RaftLog) error {
	partitions := op.CreateStreamOp.Stream.Partitions
	if stream := m.GetStream(partitions[0].Stream); stream != nil {
		return ErrStreamExists
	}
	return nil
}

// checkDeleteStreamPreconditions checks if the stream being deleted exists. If
// it doesn't, it returns ErrStreamNotFound. Otherwise, it returns nil.
func (m *metadataAPI) checkDeleteStreamPreconditions(op *proto.RaftLog) error {
	if stream := m.GetStream(op.DeleteStreamOp.Stream); stream == nil {
		return ErrStreamNotFound
	}
	return nil
}

// checkPauseStreamPreconditions checks if the stream and partitions being
// paused exist. If the stream doesn't exist, it returns ErrStreamNotFound. If
// one or more specified partitions don't exist, it returns
// ErrPartitionNotFound. Otherwise, it returns nil.
func (m *metadataAPI) checkPauseStreamPreconditions(op *proto.RaftLog) error {
	stream := m.GetStream(op.PauseStreamOp.Stream)
	if stream == nil {
		return ErrStreamNotFound
	}
	for _, partitionID := range op.PauseStreamOp.Partitions {
		if partition := stream.GetPartition(partitionID); partition == nil {
			return ErrPartitionNotFound
		}
	}
	return nil
}

// checkSetStreamReadonlyPreconditions checks if the stream and partitions being
// set readonly exist. If the stream doesn't exist, it returns
// ErrStreamNotFound. If one or more specified partitions don't exist, it
// returns ErrPartitionNotFound. Otherwise, it returns nil.
func (m *metadataAPI) checkSetStreamReadonlyPreconditions(op *proto.RaftLog) error {
	stream := m.GetStream(op.SetStreamReadonlyOp.Stream)
	if stream == nil {
		return ErrStreamNotFound
	}
	for _, partitionID := range op.SetStreamReadonlyOp.Partitions {
		if partition := stream.GetPartition(partitionID); partition == nil {
			return ErrPartitionNotFound
		}
	}
	return nil
}

// checkResumeStreamPreconditions checks if the stream and partitions to be
// resumed exist. If the stream does not exist, it returns ErrStreamNotFound.
// If any partitions do not exist, it returns ErrPartitionNotFound. Otherwise,
// it returns nil.
func (m *metadataAPI) checkResumeStreamPreconditions(op *proto.RaftLog) error {
	stream := m.GetStream(op.ResumeStreamOp.Stream)
	if stream == nil {
		return ErrStreamNotFound
	}
	for _, id := range op.ResumeStreamOp.Partitions {
		if partition := stream.GetPartition(id); partition == nil {
			return ErrPartitionNotFound
		}
	}
	return nil
}

// checkShrinkISRPreconditions checks if the partition whose ISR is being
// shrunk exists. If the stream doesn't exist, it returns ErrStreamNotFound. If
// the partition doesn't exist, it returns ErrPartitionNotFound. Otherwise, it
// returns nil.
func (m *metadataAPI) checkShrinkISRPreconditions(op *proto.RaftLog) error {
	return m.partitionExists(op.ShrinkISROp.Stream, op.ShrinkISROp.Partition)
}

// checkExpandISRPreconditions checks if the partition whose ISR is being
// expanded exists. If the stream doesn't exist, it returns ErrStreamNotFound.
// If the partition doesn't exist, it returns ErrPartitionNotFound. Otherwise,
// it returns nil.
func (m *metadataAPI) checkExpandISRPreconditions(op *proto.RaftLog) error {
	return m.partitionExists(op.ExpandISROp.Stream, op.ExpandISROp.Partition)
}

// checkChangeLeaderPreconditions checks if the partition whose leader is being
// changed exists. If the stream doesn't exist, it returns ErrStreamNotFound.
// If the partition doesn't exist, it returns ErrPartitionNotFound. Otherwise,
// it returns nil.
func (m *metadataAPI) checkChangeLeaderPreconditions(op *proto.RaftLog) error {
	return m.partitionExists(op.ChangeLeaderOp.Stream, op.ChangeLeaderOp.Partition)
}

// checkCreateConsumerGroupPreconditions checks if the group to be created
// already exists. If it does, it returns ErrConsumerGroupExists. If any of the
// initial members' requested streams do not exist, returns ErrStreamNotFound.
// Otherwise, it returns nil.
func (m *metadataAPI) checkCreateConsumerGroupPreconditions(op *proto.RaftLog) error {
	if group := m.GetConsumerGroup(op.CreateConsumerGroupOp.ConsumerGroup.Id); group != nil {
		return ErrConsumerGroupExists
	}
	for _, member := range op.CreateConsumerGroupOp.ConsumerGroup.Members {
		for _, streamName := range member.Streams {
			if stream := m.GetStream(streamName); stream == nil {
				return ErrStreamNotFound
			}
		}
	}
	return nil
}

// checkJoinConsumerGroupPreconditions checks if the group to be joined exists.
// If it does not, it returns ErrConsumerGroupNotFound. If the consumer is
// already a member of the group, returns ErrConsumerAlreadyMember. If any of
// the requested streams do not exist, returns ErrStreamNotFound. Otherwise, it
// returns nil.
func (m *metadataAPI) checkJoinConsumerGroupPreconditions(op *proto.RaftLog) error {
	group := m.GetConsumerGroup(op.JoinConsumerGroupOp.GroupId)
	if group == nil {
		return ErrConsumerGroupNotFound
	}
	if group.IsMember(op.JoinConsumerGroupOp.ConsumerId) {
		return ErrConsumerAlreadyMember
	}
	for _, streamName := range op.JoinConsumerGroupOp.Streams {
		if stream := m.GetStream(streamName); stream == nil {
			return ErrStreamNotFound
		}
	}
	return nil
}

// checkLeaveConsumerGroupPreconditions checks if the group to be joined
// exists. If it does not, it returns ErrConsumerGroupNotFound. If the consumer
// is not a member of the group, returns ErrConsumerNotMember. Otherwise, it
// returns nil.
func (m *metadataAPI) checkLeaveConsumerGroupPreconditions(op *proto.RaftLog) error {
	group := m.GetConsumerGroup(op.LeaveConsumerGroupOp.GroupId)
	if group == nil {
		return ErrConsumerGroupNotFound
	}
	if !group.IsMember(op.LeaveConsumerGroupOp.ConsumerId) {
		return ErrConsumerNotMember
	}
	return nil
}

// checkChangeGroupCoordinatorPreconditions checks if the consumer group whose
// coordinator is being changed exists. If the group doesn't exist, it returns
// ErrConsumerGroupNotFound. Otherwise, it returns nil.
func (m *metadataAPI) checkChangeGroupCoordinatorPreconditions(op *proto.RaftLog) error {
	if group := m.GetConsumerGroup(op.ChangeConsumerGroupCoordinatorOp.GroupId); group == nil {
		return ErrConsumerGroupNotFound
	}
	return nil
}

// partitionExists indicates if the given partition exists in the stream. If
// the stream doesn't exist, it returns ErrStreamNotFound. If the partition
// doesn't exist, it returns ErrPartitionNotFound.
func (m *metadataAPI) partitionExists(streamName string, partitionID int32) error {
	stream := m.GetStream(streamName)
	if stream == nil {
		return ErrStreamNotFound
	}
	if partition := stream.GetPartition(partitionID); partition == nil {
		return ErrPartitionNotFound
	}
	return nil
}

// selectPartitionLeader selects a replica from the list of replicas to act as
// leader by attempting to select the replica with the least partition
// leadership load.
func (m *metadataAPI) selectPartitionLeader(replicas []string) string {
	// Order servers by leader load.
	m.stats.RLock()
	sort.SliceStable(replicas, func(i, j int) bool {
		return m.stats.brokerLeaderLoad[replicas[i]] < m.stats.brokerLeaderLoad[replicas[j]]
	})
	m.stats.RUnlock()

	return replicas[0]
}

// selectGroupCoordinator selects a broker to act as a consumer group
// coordinator by attempting to select the broker with the least coordinator
// load.
func (m *metadataAPI) selectGroupCoordinator(candidates []string) string {
	// Order servers by coordinator load.
	m.stats.RLock()
	sort.SliceStable(candidates, func(i, j int) bool {
		return m.stats.brokerCoordinatorLoad[candidates[i]] < m.stats.brokerCoordinatorLoad[candidates[j]]
	})
	m.stats.RUnlock()

	return candidates[0]
}

// ensureTimeout ensures there is a timeout on the Context. If there is, it
// returns the Context. If there isn't it returns a new Context wrapping the
// provided one with the default timeout applied. It also returns a cancel
// function which must be invoked by the caller in all cases to avoid a Context
// leak.
func ensureTimeout(ctx context.Context, defaultTimeout time.Duration) (context.Context, context.CancelFunc) {
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
	}
	return ctx, cancel
}

// eventTimestampsToProto returns a client proto's partition event timestamps
// from a partition's event timestamps struct.
func eventTimestampsToProto(timestamps EventTimestamps) *client.PartitionEventTimestamps {
	first, latest := timestamps.firstTime, timestamps.latestTime
	result := &client.PartitionEventTimestamps{}

	// Calling UnixNano() on a zero time is undefined, so we need to make these
	// checks.
	if !first.IsZero() {
		result.FirstTimestamp = first.UnixNano()
	}
	if !latest.IsZero() {
		result.LatestTimestamp = latest.UnixNano()
	}

	return result
}

// getPartitionMetadata returns a partition's metadata.
func getPartitionMetadata(partitionID int32, partition *partition) *client.PartitionMetadata {
	leader, _ := partition.GetLeader()
	return &client.PartitionMetadata{
		Id:                         partitionID,
		Leader:                     leader,
		Replicas:                   partition.GetReplicas(),
		Isr:                        partition.GetISR(),
		HighWatermark:              partition.log.HighWatermark(),
		NewestOffset:               partition.log.NewestOffset(),
		Paused:                     partition.GetPaused(),
		Readonly:                   partition.GetReadonly(),
		MessagesReceivedTimestamps: eventTimestampsToProto(partition.MessagesReceivedTimestamps()),
		PauseTimestamps:            eventTimestampsToProto(partition.PauseTimestamps()),
		ReadonlyTimestamps:         eventTimestampsToProto(partition.ReadonlyTimestamps()),
	}
}
