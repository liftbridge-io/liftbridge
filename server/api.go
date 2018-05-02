package server

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	client "github.com/tylertreat/go-jetbridge/proto"
	"golang.org/x/net/context"

	"github.com/tylertreat/jetbridge/server/proto"
)

const (
	raftApplyTimeout     = 30 * time.Second
	defaultFetchMaxBytes = 1048576
)

var envelopeCookie = []byte("jetb")

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

func (a *apiServer) ConsumeStream(req *client.ConsumeStreamRequest, out client.API_ConsumeStreamServer) error {
	a.logger.Debugf("ConsumeStream[subject=%s, name=%s, offset=%d]", req.Subject, req.Name, req.Offset)
	stream := a.metadata.GetStream(req.Subject, req.Name)
	if stream == nil {
		return errors.New("No such stream")
	}

	if stream.Leader != a.config.Clustering.NodeID {
		a.logger.Error("Failed to fetch stream: node is not stream leader")
		return errors.New("Node is not stream leader")
	}

	ch, errCh, err := a.consumeStream(out.Context(), stream, req)
	if err != nil {
		a.logger.Errorf("Failed to fetch stream: %v", err)
		return err
	}
	for {
		select {
		case <-out.Context().Done():
			return nil
		case m := <-ch:
			if err := out.Send(m); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		}
	}
}

func (a *apiServer) Publish(stream client.API_PublishServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		data, err := req.Msg.Marshal()
		if err != nil {
			return err
		}
		buf := make([]byte, 4+len(data))
		copy(buf, envelopeCookie)
		copy(buf[4:], data)

		resp := &client.PublishResponse{}
		if req.Ack {
			timeout := 5 * time.Second
			if req.AckWait != 0 {
				timeout = time.Duration(req.AckWait) * time.Millisecond
			}
			ack, err := a.publishWithAck(req.Msg.Subject, req.Msg.Reply, buf, timeout)
			if err == nil {
				resp.Ack = ack
			} else if err != nats.ErrTimeout {
				return err
			}
		} else {
			if err := a.nats.PublishRequest(req.Msg.Subject, req.Msg.Reply, buf); err != nil {
				return err
			}
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (a *apiServer) publishWithAck(subject, reply string, data []byte, ackTimeout time.Duration) (*client.Ack, error) {
	ackInbox := nats.NewInbox()
	sub, err := a.nats.SubscribeSync(ackInbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	if err := a.nats.PublishRequest(subject, reply, data); err != nil {
		return nil, err
	}
	resp, err := sub.NextMsg(ackTimeout)
	if err != nil {
		return nil, err
	}
	ack := &client.Ack{}
	if err := ack.Unmarshal(resp.Data); err != nil {
		return nil, err
	}
	return ack, nil
}

func (a *apiServer) consumeStream(ctx context.Context, stream *stream, req *client.ConsumeStreamRequest) (<-chan *client.Message, <-chan error, error) {
	var (
		ch          = make(chan *client.Message)
		errCh       = make(chan error)
		reader, err = stream.log.NewReaderContext(ctx, req.Offset)
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create log reader")
	}

	go func() {
		headersBuf := make([]byte, 12)
		for {
			if _, err := reader.Read(headersBuf); err != nil {
				errCh <- err
				return
			}
			offset := int64(proto.Encoding.Uint64(headersBuf[0:]))
			size := proto.Encoding.Uint32(headersBuf[8:])
			buf := make([]byte, size)
			if _, err := reader.Read(buf); err != nil {
				errCh <- err
				return
			}
			m := &proto.Message{}
			decoder := proto.NewDecoder(buf)
			if err := m.Decode(decoder); err != nil {
				panic(err)
			}
			var (
				msg = &client.Message{
					Offset:    offset,
					Key:       m.Key,
					Value:     m.Value,
					Timestamp: m.Timestamp.UnixNano(),
					Headers:   m.Headers,
					Subject:   string(m.Headers["subject"]),
					Reply:     string(m.Headers["reply"]),
				}
			)
			ch <- msg
		}
	}()

	return ch, errCh, nil
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
