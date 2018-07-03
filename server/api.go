package server

import (
	"fmt"
	"time"

	client "github.com/tylertreat/go-liftbridge/liftbridge-grpc"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tylertreat/liftbridge/server/commitlog"
)

const raftApplyTimeout = 30 * time.Second

type apiServer struct {
	*Server
}

func (a *apiServer) CreateStream(ctx context.Context, req *client.CreateStreamRequest) (
	*client.CreateStreamResponse, error) {

	resp := &client.CreateStreamResponse{}
	a.logger.Debugf("api: CreateStream [subject=%s, name=%s, replicationFactor=%d]",
		req.Subject, req.Name, req.ReplicationFactor)

	if err := a.metadata.CreateStream(ctx, req); err != nil {
		if err.Code() != codes.AlreadyExists {
			a.logger.Errorf("api: Failed to create stream: %v", err.Err())
		}
		return nil, err.Err()
	}

	return resp, nil
}

func (a *apiServer) Subscribe(req *client.SubscribeRequest, out client.API_SubscribeServer) error {
	a.logger.Debugf("api: Subscribe [subject=%s, name=%s, start=%s, offset=%d]",
		req.Subject, req.Name, req.StartPosition, req.StartOffset)
	stream := a.metadata.GetStream(req.Subject, req.Name)
	if stream == nil {
		a.logger.Errorf("api: Failed to subscribe to stream [subject=%s, name=%s]: no such stream",
			req.Subject, req.Name)
		return status.Error(codes.NotFound, "No such stream")
	}

	leader, _ := stream.GetLeader()
	if leader != a.config.Clustering.ServerID {
		a.logger.Errorf("api: Failed to subscribe to stream %s: server not stream leader", stream)
		return status.Error(codes.FailedPrecondition, "Server not stream leader")
	}

	cancel := make(chan struct{})
	defer close(cancel)
	ch, errCh, err := a.subscribe(out.Context(), stream, req, cancel)
	if err != nil {
		a.logger.Errorf("api: Failed to subscribe to stream %s: %v", stream, err.Err())
		return err.Err()
	}

	// Send an empty message which signals the subscription was successfully
	// created.
	if err := out.Send(&client.Message{}); err != nil {
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
			return err.Err()
		}
	}
}

func (a *apiServer) FetchMetadata(ctx context.Context, req *client.FetchMetadataRequest) (
	*client.FetchMetadataResponse, error) {
	a.logger.Debugf("api: FetchMetadata %s", req.Streams)

	resp, err := a.metadata.FetchMetadata(ctx, req)
	if err != nil {
		a.logger.Errorf("api: Failed to fetch metadata: %v", err.Err())
		return nil, err.Err()
	}

	return resp, nil
}

func (a *apiServer) subscribe(ctx context.Context, stream *stream,
	req *client.SubscribeRequest, cancel chan struct{}) (
	<-chan *client.Message, <-chan *status.Status, *status.Status) {

	var (
		ch          = make(chan *client.Message)
		errCh       = make(chan *status.Status)
		reader, err = stream.log.NewReaderCommitted(ctx, req.StartOffset)
	)
	if err != nil {
		return nil, nil, status.New(codes.Internal, fmt.Sprintf("Failed to create stream reader: %v", err))
	}

	a.startGoroutine(func() {
		headersBuf := make([]byte, 12)
		for {
			// TODO: this could be more efficient.
			m, offset, err := commitlog.ReadMessage(reader, headersBuf)
			if err != nil {
				select {
				case errCh <- status.Convert(err):
				case <-cancel:
				}
				return
			}
			headers := m.Headers()
			var (
				msg = &client.Message{
					Offset:    offset,
					Key:       m.Key(),
					Value:     m.Value(),
					Timestamp: m.Timestamp(),
					Headers:   headers,
					Subject:   string(headers["subject"]),
					Reply:     string(headers["reply"]),
				}
			)
			select {
			case ch <- msg:
			case <-cancel:
				return
			}
		}
	})

	return ch, errCh, nil
}
