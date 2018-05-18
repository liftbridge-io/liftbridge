package server

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	client "github.com/tylertreat/go-liftbridge/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tylertreat/liftbridge/server/proto"
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
	a.logger.Debugf("api: CreateStream [subject=%s, name=%s, replicationFactor=%d]",
		req.Subject, req.Name, req.ReplicationFactor)

	if err := a.metadata.CreateStream(ctx, req); err != nil {
		a.logger.Errorf("api: Failed to create stream: %v", err.Err())
		return nil, err.Err()
	}

	return resp, nil
}

func (a *apiServer) ConsumeStream(req *client.ConsumeStreamRequest, out client.API_ConsumeStreamServer) error {
	a.logger.Debugf("api: ConsumeStream [subject=%s, name=%s, offset=%d]", req.Subject, req.Name, req.Offset)
	stream := a.metadata.GetStream(req.Subject, req.Name)
	if stream == nil {
		return errors.New("No such stream")
	}

	if stream.Leader != a.config.Clustering.ServerID {
		a.logger.Error("api: Failed to fetch stream: node is not stream leader")
		return errors.New("Node is not stream leader")
	}

	ch, errCh, err := a.consumeStream(out.Context(), stream, req)
	if err != nil {
		a.logger.Errorf("api: Failed to fetch stream: %v", err.Err())
		return err.Err()
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

func (a *apiServer) consumeStream(ctx context.Context, stream *stream, req *client.ConsumeStreamRequest) (<-chan *client.Message, <-chan *status.Status, *status.Status) {
	var (
		ch          = make(chan *client.Message)
		errCh       = make(chan *status.Status)
		reader, err = stream.log.NewReaderCommitted(ctx, req.Offset)
	)
	if err != nil {
		return nil, nil, status.New(codes.Internal, fmt.Sprintf("Failed to create stream reader: %v", err))
	}

	go func() {
		headersBuf := make([]byte, 12)
		for {
			buf, offset, err := consumeStreamMessageSet(reader, headersBuf)
			if err != nil {
				errCh <- status.Convert(err)
				return
			}
			// TODO: this could be more efficient.
			var (
				ms      = &proto.MessageSet{}
				decoder = proto.NewDecoder(buf)
			)
			if err := ms.Decode(decoder); err != nil {
				panic(err)
			}
			var (
				m   = ms.Messages[0]
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
