package server

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	client "github.com/tylertreat/go-jetbridge/proto"
	"golang.org/x/net/context"

	"github.com/tylertreat/jetbridge/server/proto"
)

const (
	raftApplyTimeout     = 30 * time.Second
	defaultFetchMaxBytes = 1048576
)

type apiServer struct {
	*Server
}

func (a *apiServer) CreateStream(ctx context.Context, req *client.CreateStreamRequest) (*client.CreateStreamResponse, error) {
	resp := &client.CreateStreamResponse{}
	a.logger.Debugf("CreateStream[subject=%s, name=%s, replicationFactor=%d]",
		req.Subject, req.Name, req.ReplicationFactor)
	if !a.isLeader() {
		// TODO: forward request to leader.
		resp.Success = false
		resp.Error = "Node is not metadata leader"
		a.logger.Error("Failed to create stream: node is not metadata leader")
		return resp, nil
	}

	if err := a.createStream(ctx, req); err != nil {
		resp.Success = false
		resp.Error = err.Error()
		a.logger.Errorf("Failed to create stream: %v", err)
		return resp, nil
	}

	resp.Success = true
	return resp, nil
}

func (a *apiServer) FetchStream(ctx context.Context, req *client.FetchStreamRequest) (*client.FetchStreamResponse, error) {
	resp := &client.FetchStreamResponse{}
	a.logger.Debugf("FetchStream[subject=%s, name=%s, offset=%d]", req.Subject, req.Name, req.Offset)
	a.mu.RLock()
	streams := a.streams[req.Subject]
	if streams == nil {
		a.mu.RUnlock()
		resp.Success = false
		resp.Error = "No such stream"
		return resp, nil
	}
	stream := streams[req.Name]
	a.mu.RUnlock()
	if stream == nil {
		a.mu.RUnlock()
		resp.Success = false
		resp.Error = "No such stream"
		return resp, nil
	}

	if stream.Leader != a.config.Clustering.NodeID {
		resp.Success = false
		resp.Error = "Node is not stream leader"
		a.logger.Error("Failed to fetch stream: node is not stream leader")
		return resp, nil
	}

	var err error
	resp, err = a.fetchStream(ctx, stream, req)
	if err != nil {
		resp.Success = false
		resp.Error = err.Error()
		a.logger.Errorf("Failed to fetch stream: %v", err)
		return resp, nil
	}

	return resp, nil
}

func (a *apiServer) fetchStream(ctx context.Context, stream *stream, req *client.FetchStreamRequest) (*client.FetchStreamResponse, error) {
	if req.MinBytes <= 0 {
		req.MinBytes = 1
	}
	if req.MaxBytes < 0 {
		return nil, fmt.Errorf("Invalid maxBytes %d", req.MaxBytes)
	}
	if req.MaxBytes == 0 {
		req.MaxBytes = defaultFetchMaxBytes
	}

	fmt.Println(req.Offset, req.MaxBytes)
	reader, err := stream.log.NewReader(req.Offset, req.MaxBytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create log reader")
	}
	var (
		buf      = new(bytes.Buffer)
		n        int32
		received = time.Now()
	)
	for n < req.MinBytes {
		if req.MaxWait != 0 && int32(time.Since(received).Nanoseconds()/1e6) > req.MaxWait {
			break
		}
		num, err := io.Copy(buf, reader)
		if err != nil && err != io.EOF {
			return nil, errors.Wrap(err, "failed to read from log")
		}
		n += int32(num)
		if err == io.EOF {
			break
		}
	}
	return &client.FetchStreamResponse{
		Success: true,
		Data:    buf.Bytes(),
		Hw:      stream.log.NewestOffset(),
	}, nil
}

func (a *apiServer) createStream(ctx context.Context, req *client.CreateStreamRequest) error {
	// Select replicationFactor nodes to participate in the stream.
	// TODO: Currently this selection is random but could be made more
	// intelligent, e.g. selecting based on current load.
	participants, err := a.getStreamParticipants(req.ReplicationFactor)
	if err != nil {
		errors.Wrap(err, "failed to select stream participants")
	}

	// Select a leader at random.
	leader := participants[rand.Intn(len(participants))]

	// Replicate stream create through Raft.
	op := &proto.RaftLog{
		Op: proto.RaftLog_CREATE_STREAM,
		CreateStreamOp: &proto.CreateStreamOp{
			Stream: &proto.Stream{
				Subject:           req.Subject,
				Name:              req.Name,
				ConsumerGroup:     req.ConsumerGroup,
				ReplicationFactor: req.ReplicationFactor,
				Participants:      participants,
				Leader:            leader,
			},
		},
	}

	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}

	// Wait on result of replication.
	return a.raft.Apply(data, raftApplyTimeout).Error()
}

func (a *apiServer) getStreamParticipants(replicationFactor int32) ([]string, error) {
	ids, err := a.getClusterServerIDs()
	if err != nil {
		return nil, err
	}
	if replicationFactor <= 0 {
		return nil, fmt.Errorf("Invalid replicationFactor %d", replicationFactor)
	}
	if replicationFactor > int32(len(ids)) {
		return nil, fmt.Errorf("Invalid replicationFactor %d, cluster size %d", replicationFactor, len(ids))
	}
	var (
		indexes      = rand.Perm(len(ids))
		participants = make([]string, replicationFactor)
	)
	for i := int32(0); i < replicationFactor; i++ {
		participants[i] = ids[indexes[i]]
	}
	return participants, nil
}

func (a *apiServer) getClusterServerIDs() ([]string, error) {
	future := a.raft.GetConfiguration()
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
