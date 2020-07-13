package server

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	client "github.com/liftbridge-io/liftbridge-api/go"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

const (
	defaultPropagateTimeout       = 5 * time.Second
	maxReplicationFactor    int32 = -1
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
)

// leaderReport tracks witnesses for a partition leader. Witnesses are replicas
// which have reported the leader as unresponsive. If a quorum of replicas
// report the leader within a bounded period of time, the controller will
// select a new leader.
type leaderReport struct {
	mu              sync.Mutex
	partition       *partition
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
		isrSize      = l.partition.ISRSize() - 1
		leaderFailed = len(l.witnessReplicas) > isrSize/2
	)

	if leaderFailed {
		if l.timer != nil {
			l.timer.Stop()
		}
		return l.api.electNewPartitionLeader(l.partition)
	}

	if l.timer != nil {
		l.timer.Reset(l.api.config.Clustering.ReplicaMaxLeaderTimeout)
	} else {
		l.timer = time.AfterFunc(
			l.api.config.Clustering.ReplicaMaxLeaderTimeout, func() {
				l.api.mu.Lock()
				delete(l.api.leaderReports, l.partition)
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
	streams         map[string]*stream
	mu              sync.RWMutex
	leaderReports   map[*partition]*leaderReport
	cachedBrokers   []*client.Broker
	cachedServerIDs map[string]struct{}
	lastCached      time.Time
}

func newMetadataAPI(s *Server) *metadataAPI {
	return &metadataAPI{
		Server:        s,
		streams:       make(map[string]*stream),
		leaderReports: make(map[*partition]*leaderReport),
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
	connectionAddress := m.config.GetConnectionAddress()
	brokers := []*client.Broker{{
		Id:   m.config.Clustering.ServerID,
		Host: connectionAddress.Host,
		Port: int32(connectionAddress.Port),
	}}

	// Make sure there is a deadline on the request.
	ctx, cancel := ensureTimeout(ctx, defaultPropagateTimeout)
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
	m.ncRaft.PublishRequest(m.getServerInfoInbox(), inbox, queryReq)

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
			Id:   queryResp.Id,
			Host: queryResp.Host,
			Port: queryResp.Port,
		})
	}

	return brokers, nil
}

// createMetadataResponse creates a FetchMetadataResponse and populates it with
// stream metadata. If the provided list of stream names is empty, it will
// populate metadata for all streams. Otherwise, it populates only the
// specified streams.
func (m *metadataAPI) createMetadataResponse(streams []string) *client.FetchMetadataResponse {
	// If no stream names were provided, fetch metadata for all streams.
	if len(streams) == 0 {
		for _, stream := range m.GetStreams() {
			streams = append(streams, stream.GetName())
		}
	}

	metadata := make([]*client.StreamMetadata, len(streams))

	for i, name := range streams {
		stream := m.GetStream(name)
		if stream == nil {
			// Stream does not exist.
			metadata[i] = &client.StreamMetadata{
				Name:  name,
				Error: client.StreamMetadata_UNKNOWN_STREAM,
			}
		} else {
			partitions := make(map[int32]*client.PartitionMetadata)
			for id, partition := range stream.GetPartitions() {
				leader, _ := partition.GetLeader()
				partitions[id] = &client.PartitionMetadata{
					Id:       id,
					Leader:   leader,
					Replicas: partition.GetReplicas(),
					Isr:      partition.GetISR(),
					Paused:   partition.GetPaused(),
				}
			}
			metadata[i] = &client.StreamMetadata{
				Name:       name,
				Subject:    stream.GetSubject(),
				Partitions: partitions,
			}
		}
	}

	return &client.FetchMetadataResponse{Metadata: metadata}
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

		// Select a leader at random.
		leader := selectRandomReplica(replicas)

		partition.Replicas = replicas
		partition.Isr = replicas
		partition.Leader = leader
	}

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
	reported := m.leaderReports[partition]
	if reported == nil {
		reported = &leaderReport{
			partition:       partition,
			witnessReplicas: make(map[string]struct{}),
			api:             m,
		}
		m.leaderReports[partition] = reported
	}
	m.mu.Unlock()

	return reported.addWitness(req.Replica)
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

	// Replicate stream the readonly flag through Raft.
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

// AddStream adds the given stream and its partitions to the metadata store. It
// returns an error if a stream with the same name or any partitions with the
// same ID for the stream already exist. If the stream is recovered, this will
// not start the partitions until recovery completes. Partitions will also not
// be started if they are currently paused.
func (m *metadataAPI) AddStream(protoStream *proto.Stream, recovered bool) (*stream, error) {
	if len(protoStream.Partitions) == 0 {
		return nil, errors.New("stream has no partitions")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.streams[protoStream.Name]
	if ok {
		return nil, ErrStreamExists
	}

	config := protoStream.GetConfig()
	stream := newStream(protoStream.Name, protoStream.Subject, config)
	m.streams[protoStream.Name] = stream

	for _, partition := range protoStream.Partitions {
		if err := m.addPartition(stream, partition, recovered, config); err != nil {
			delete(m.streams, protoStream.Name)
			return nil, err
		}
	}

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
	partition, err := m.newPartition(partition.Partition, recovered, stream.GetConfig())
	if err != nil {
		return nil, err
	}
	stream.SetPartition(id, partition)

	// Start leader/follower loop if necessary.
	leader, epoch := partition.GetLeader()
	err = partition.SetLeader(leader, epoch)
	return partition, err
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

// Reset closes all streams and clears all existing state in the metadata
// store.
func (m *metadataAPI) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, stream := range m.getStreams() {
		if err := stream.Close(); err != nil {
			return err
		}
	}
	m.streams = make(map[string]*stream)
	for _, report := range m.leaderReports {
		report.cancel()
	}
	m.leaderReports = make(map[*partition]*leaderReport)
	return nil
}

// CloseStream close a streams and clears corresponding state in the metadata
// store.
func (m *metadataAPI) CloseAndDeleteStream(stream *stream) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := stream.Delete()
	if err != nil {
		return errors.Wrap(err, "failed to delete stream")
	}

	// Remove the (now empty) stream data directory
	streamDataDir := filepath.Join(m.Server.config.DataDir, "streams", stream.GetName())
	err = os.Remove(streamDataDir)
	if err != nil {
		return errors.Wrap(err, "failed to delete stream data directory")
	}

	delete(m.streams, stream.GetName())

	for _, partition := range stream.GetPartitions() {
		report, ok := m.leaderReports[partition]
		if ok {
			report.cancel()
			delete(m.leaderReports, partition)
		}
	}

	return nil
}

// LostLeadership should be called when the server loses metadata leadership.
func (m *metadataAPI) LostLeadership() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, report := range m.leaderReports {
		report.cancel()
	}
	m.leaderReports = make(map[*partition]*leaderReport)
}

func (m *metadataAPI) getStreams() []*stream {
	streams := make([]*stream, 0, len(m.streams))
	for _, stream := range m.streams {
		streams = append(streams, stream)
	}
	return streams
}

// getPartitionReplicas selects replicationFactor replicas to participate in
// the stream partition.
func (m *metadataAPI) getPartitionReplicas(replicationFactor int32) ([]string, *status.Status) {
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
func (m *metadataAPI) electNewPartitionLeader(partition *partition) *status.Status {
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

	// Select a new leader at random.
	leader = selectRandomReplica(candidates)

	// Replicate leader change through Raft.
	op := &proto.RaftLog{
		Op: proto.Op_CHANGE_LEADER,
		ChangeLeaderOp: &proto.ChangeLeaderOp{
			Stream: partition.Stream,
			Leader: leader,
		},
	}

	// Wait on result of replication.
	future, err := m.getRaft().applyOperation(context.TODO(), op, m.checkChangeLeaderPreconditions)
	if err != nil {
		return status.Newf(codes.FailedPrecondition, err.Error())
	}
	if err := future.Error(); err != nil {
		return status.Newf(codes.Internal, "Failed to replicate leader change: %v", err.Error())
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
	return m.propagateRequest(ctx, propagate)
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
	return m.propagateRequest(ctx, propagate)
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
	return m.propagateRequest(ctx, propagate)
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
	return m.propagateRequest(ctx, propagate)
}

// propagateShrinkISR forwards a ShrinkISR request to the metadata leader. The
// bool indicates if this server has since become leader and the request should
// be performed locally. A Status is returned if the propagated request failed.
func (m *metadataAPI) propagateShrinkISR(ctx context.Context, req *proto.ShrinkISROp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:          proto.Op_SHRINK_ISR,
		ShrinkISROp: req,
	}
	return m.propagateRequest(ctx, propagate)
}

// propagateExpandISR forwards a ExpandISR request to the metadata leader. The
// bool indicates if this server has since become leader and the request should
// be performed locally. A Status is returned if the propagated request failed.
func (m *metadataAPI) propagateExpandISR(ctx context.Context, req *proto.ExpandISROp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:          proto.Op_EXPAND_ISR,
		ExpandISROp: req,
	}
	return m.propagateRequest(ctx, propagate)
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
	return m.propagateRequest(ctx, propagate)
}

// propagateSetStreamReadonly forwards a ReportLeader request to the metadata
// leader. The bool indicates if this server has since become leader and the
// request should be performed locally. A Status is returned if the propagated
// request failed.
func (m *metadataAPI) propagateSetStreamReadonly(ctx context.Context, req *proto.SetStreamReadonlyOp) (bool, *status.Status) {
	propagate := &proto.PropagatedRequest{
		Op:                  proto.Op_SET_STREAM_READONLY,
		SetStreamReadonlyOp: req,
	}
	return m.propagateRequest(ctx, propagate)
}

// propagateRequest forwards a metadata request to the metadata leader. The
// bool indicates if this server has since become leader and the request should
// be performed locally. A Status is returned if the propagated request failed.
func (m *metadataAPI) propagateRequest(ctx context.Context, req *proto.PropagatedRequest) (bool, *status.Status) {
	// Check if there is currently a metadata leader.
	isLeader, err := m.waitForMetadataLeader(ctx)
	if err != nil {
		return false, status.New(codes.Internal, err.Error())
	}
	// This server has since become metadata leader, so the request should be
	// performed locally.
	if isLeader {
		return true, nil
	}

	data, err := proto.MarshalPropagatedRequest(req)
	if err != nil {
		panic(err)
	}

	ctx, cancel := ensureTimeout(ctx, defaultPropagateTimeout)
	defer cancel()

	resp, err := m.nc.RequestWithContext(ctx, m.getPropagateInbox(), data)
	if err != nil {
		return false, status.New(codes.Internal, err.Error())
	}

	r, err := proto.UnmarshalPropagatedResponse(resp.Data)
	if err != nil {
		m.logger.Errorf("metadata: Invalid response for propagated request: %v", err)
		return false, status.New(codes.Internal, "invalid response")
	}
	if r.Error != nil {
		return false, status.New(codes.Code(r.Error.Code), r.Error.Msg)
	}

	return false, nil
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

// selectRandomReplica selects a random replica from the list of replicas.
func selectRandomReplica(replicas []string) string {
	return replicas[rand.Intn(len(replicas))]
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
