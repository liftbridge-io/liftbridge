package server

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	client "github.com/tylertreat/go-jetbridge/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tylertreat/jetbridge/server/proto"
)

const (
	defaultPropagateTimeout = 5 * time.Second
	streamsBucket           = "streams"
)

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
		isrSize      = l.stream.isrSize() - 1
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
	db            *bolt.DB
	dbFile        string
}

func newMetadataAPI(s *Server) (*metadataAPI, error) {
	path := filepath.Join(s.config.DataPath, "metadata")
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create database directory")
	}
	var (
		dbFile  = filepath.Join(path, "metadata.db")
		db, err = bolt.Open(dbFile, 0666, nil)
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open metadata database")
	}

	m := &metadataAPI{
		Server:        s,
		streams:       make(map[string]subjectStreams),
		leaderReports: make(map[*stream]*leaderReported),
		db:            db,
		dbFile:        dbFile,
	}
	err = m.initializeDB()
	return m, err
}

func (m *metadataAPI) Recover() (int, error) {
	// Restore any previous state.
	recovered := 0
	if err := m.db.View(func(tx *bolt.Tx) error {
		streams := tx.Bucket([]byte(streamsBucket))
		return streams.ForEach(func(subject, _ []byte) error {
			return streams.Bucket(subject).ForEach(func(name, protobuf []byte) error {
				stream := &proto.Stream{}
				if err := stream.Unmarshal(protobuf); err != nil {
					panic(err)
				}
				_, err := m.addStream(stream, true)
				if err == nil {
					recovered++
				}
				return err
			})
		})
	}); err != nil {
		return 0, err
	}
	return recovered, nil
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
	leader, epoch := stream.getLeader()
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
	leader, epoch := stream.getLeader()
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

func (m *metadataAPI) AddStream(protoStream *proto.Stream) (*stream, error) {
	return m.addStream(protoStream, false)
}

func (m *metadataAPI) addStream(protoStream *proto.Stream, recovered bool) (*stream, error) {
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
	stream, err := m.newStream(protoStream)
	if err != nil {
		return nil, err
	}

	// Write the proto to the db if we're not recovering.
	if !recovered {
		serialized := stream.Marshal()
		if err := m.db.Update(func(tx *bolt.Tx) error {
			streams := tx.Bucket([]byte(streamsBucket))
			b, err := streams.CreateBucketIfNotExists([]byte(stream.Subject))
			if err != nil {
				return err
			}
			return b.Put([]byte(stream.Name), serialized)
		}); err != nil {
			return nil, err
		}
	}

	streams[stream.Name] = stream

	// Start leader/follower loop if necessary.
	leader, epoch := stream.getLeader()
	err = stream.SetLeader(leader, epoch)
	return stream, err
}

func (m *metadataAPI) CheckpointStream(stream *stream) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	streams := m.streams[stream.Subject]
	if streams == nil {
		return errors.New("stream does not exist")
	}
	if _, ok := streams[stream.Name]; !ok {
		return errors.New("stream does not exist")
	}

	data := stream.Marshal()
	return m.db.Update(func(tx *bolt.Tx) error {
		streams := tx.Bucket([]byte(streamsBucket))
		b := streams.Bucket([]byte(stream.Subject))
		if b == nil {
			return errors.New("stream does not exist")
		}
		return b.Put([]byte(stream.Name), data)
	})
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
	if err := m.close(); err != nil {
		return err
	}
	if err := os.Remove(m.dbFile); err != nil {
		return err
	}
	db, err := bolt.Open(m.dbFile, 0666, nil)
	if err != nil {
		return err
	}
	m.db = db
	return m.initializeDB()
}

func (m *metadataAPI) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.close()
}

func (m *metadataAPI) close() error {
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
	if err := m.db.Close(); err != nil {
		return err
	}
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
	isr := stream.getISR()
	if len(isr) == 1 {
		return status.New(codes.FailedPrecondition, "No ISR candidates")
	}
	var (
		candidates = make([]string, 0, len(isr)-1)
		leader, _  = stream.getLeader()
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

func (m *metadataAPI) initializeDB() error {
	return m.db.Update(func(tx *bolt.Tx) error {
		// Initialize streams bucket if it doesn't already exist.
		_, err := tx.CreateBucketIfNotExists([]byte(streamsBucket))
		return err
	})
}

// selectRandomReplica selects a random replica from the list of replicas.
func selectRandomReplica(replicas []string) string {
	return replicas[rand.Intn(len(replicas))]
}
