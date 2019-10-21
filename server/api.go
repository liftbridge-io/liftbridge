package server

import (
	"context"
	"fmt"
	"time"

	client "github.com/liftbridge-io/liftbridge-grpc/go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liftbridge-io/liftbridge/server/proto"
)

const raftApplyTimeout = 30 * time.Second

// apiServer implements the gRPC server interface clients interact with.
type apiServer struct {
	*Server
}

// CreateStream creates a new stream attached to a NATS subject. It returns an
// AlreadyExists status code if a stream with the given subject and name
// already exists.
func (a *apiServer) CreateStream(ctx context.Context, req *client.CreateStreamRequest) (
	*client.CreateStreamResponse, error) {

	resp := &client.CreateStreamResponse{}
	if req.ReplicationFactor == 0 {
		req.ReplicationFactor = 1
	}
	if req.Partitions == 0 {
		req.Partitions = 1
	}
	a.logger.Debugf("api: CreateStream [subject=%s, name=%s, partitions=%d, replicationFactor=%d]",
		req.Subject, req.Name, req.Partitions, req.ReplicationFactor)

	var err error
	for i := int32(0); i < req.Partitions; i++ {
		if e := a.metadata.CreatePartition(ctx, &proto.CreatePartitionOp{
			Partition: &proto.Partition{
				Subject:           req.Subject,
				Stream:            req.Name,
				Group:             req.Group,
				ReplicationFactor: req.ReplicationFactor,
				Id:                i,
			},
		}); e != nil {
			// In the case of partial failure, let the caller retry.
			if e.Code() == codes.AlreadyExists {
				err = e.Err()
			} else {
				a.logger.Errorf("api: Failed to create stream partition %d: %v", i, e.Err())
				return nil, e.Err()
			}
		}
	}

	return resp, err
}

// Subscribe creates an ephemeral subscription for the given stream partition.
// It begins to receive messages starting at the given offset and waits for new
// messages when it reaches the end of the partition. Use the request context
// to close the subscription.
func (a *apiServer) Subscribe(req *client.SubscribeRequest, out client.API_SubscribeServer) error {
	a.logger.Debugf("api: Subscribe [stream=%s, partition=%d, start=%s, offset=%d, timestamp=%d]",
		req.Stream, req.Partition, req.StartPosition, req.StartOffset, req.StartTimestamp)

	partition := a.metadata.GetPartition(req.Stream, req.Partition)
	if partition == nil {
		a.logger.Errorf("api: Failed to subscribe to partition "+
			"[stream=%s, partition=%d]: no such partition",
			req.Stream, req.Partition)
		return status.Error(codes.NotFound, "No such partition")
	}

	leader, _ := partition.GetLeader()
	if leader != a.config.Clustering.ServerID {
		a.logger.Errorf("api: Failed to subscribe to partition %s: server not stream leader", partition)
		return status.Error(codes.FailedPrecondition, "Server not partition leader")
	}

	cancel := make(chan struct{})
	defer close(cancel)
	ch, errCh, err := a.subscribe(out.Context(), partition, req, cancel)
	if err != nil {
		a.logger.Errorf("api: Failed to subscribe to partition %s: %v", partition, err.Err())
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

// FetchMetadata retrieves the latest cluster metadata, including stream broker
// information.
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

// Publish a new message to a subject. If the AckPolicy is not NONE and a
// deadline is provided, this will synchronously block until the ack is
// received. If the ack is not received in time, a DeadlineExceeded status code
// is returned.
func (a *apiServer) Publish(ctx context.Context, req *client.PublishRequest) (
	*client.PublishResponse, error) {
	if req.Message == nil {
		a.logger.Errorf("api: Failed to publish message: message is nil")
		return nil, status.Error(codes.InvalidArgument, "Message is nil")
	}
	a.logger.Debugf("api: Publish [subject=%s]", req.Message.Subject)

	if req.Message.AckInbox == "" {
		req.Message.AckInbox = nuid.Next()
	}

	msg, err := req.Message.Marshal()
	if err != nil {
		a.logger.Errorf("api: Failed to publish message: %v", err.Error())
		return nil, err
	}

	buf := make([]byte, envelopeCookieLen+len(msg))
	copy(buf[0:], envelopeCookie)
	copy(buf[envelopeCookieLen:], msg)

	// If AckPolicy is NONE or a timeout isn't specified, then we will fire and
	// forget.
	var (
		resp           = new(client.PublishResponse)
		_, hasDeadline = ctx.Deadline()
	)
	if req.Message.AckPolicy == client.AckPolicy_NONE || !hasDeadline {
		if err := a.ncPublishes.Publish(req.Message.Subject, buf); err != nil {
			a.logger.Errorf("api: Failed to publish message: %v", err)
			return nil, err
		}
		return resp, nil
	}

	// Otherwise we need to publish and wait for the ack.
	resp.Ack, err = a.publishSync(ctx, req.Message.Subject, req.Message.AckInbox, buf)
	return resp, err
}

func (a *apiServer) publishSync(ctx context.Context, subject,
	ackInbox string, msg []byte) (*client.Ack, error) {

	sub, err := a.ncPublishes.SubscribeSync(ackInbox)
	if err != nil {
		a.logger.Errorf("api: Failed to subscribe to ack inbox: %v", err)
		return nil, err
	}
	if err := sub.AutoUnsubscribe(1); err != nil {
		a.logger.Errorf("api: Failed to auto unsubscribe from ack inbox: %v", err)
		return nil, err
	}

	if err := a.ncPublishes.Publish(subject, msg); err != nil {
		a.logger.Errorf("api: Failed to publish message: %v", err)
		return nil, err
	}

	ackMsg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		if err == nats.ErrTimeout {
			a.logger.Errorf("api: Ack for publish timed out")
			err = status.Error(codes.DeadlineExceeded, err.Error())
		} else {
			a.logger.Errorf("api: Failed to get ack for publish: %v", err)
		}
		return nil, err
	}

	ack := new(client.Ack)
	if err := ack.Unmarshal(ackMsg.Data); err != nil {
		a.logger.Errorf("api: Invalid ack for publish: %v", err)
		return nil, err
	}
	return ack, nil
}

// subscribe sets up a subscription on the given partition and begins sending
// messages on the returned channel. The subscription will run until the cancel
// channel is closed, the context is canceled, or an error is returned
// asynchronously on the status channel.
func (a *apiServer) subscribe(ctx context.Context, partition *partition,
	req *client.SubscribeRequest, cancel chan struct{}) (
	<-chan *client.Message, <-chan *status.Status, *status.Status) {

	var startOffset int64
	switch req.StartPosition {
	case client.StartPosition_OFFSET:
		startOffset = req.StartOffset
	case client.StartPosition_TIMESTAMP:
		offset, err := partition.log.OffsetForTimestamp(req.StartTimestamp)
		if err != nil {
			return nil, nil, status.New(
				codes.Internal, fmt.Sprintf("Failed to lookup offset for timestamp: %v", err))
		}
		startOffset = offset
	case client.StartPosition_EARLIEST:
		startOffset = partition.log.OldestOffset()
	case client.StartPosition_LATEST:
		startOffset = partition.log.NewestOffset()
	case client.StartPosition_NEW_ONLY:
		startOffset = partition.log.NewestOffset() + 1
	default:
		return nil, nil, status.New(
			codes.InvalidArgument,
			fmt.Sprintf("Unknown StartPosition %s", req.StartPosition))
	}

	// If log is empty, next offset will be 0.
	if startOffset < 0 {
		startOffset = 0
	}

	var (
		ch          = make(chan *client.Message)
		errCh       = make(chan *status.Status)
		reader, err = partition.log.NewReader(startOffset, false)
	)
	if err != nil {
		return nil, nil, status.New(
			codes.Internal, fmt.Sprintf("Failed to create stream reader: %v", err))
	}

	a.startGoroutine(func() {
		headersBuf := make([]byte, 28)
		for {
			// TODO: this could be more efficient.
			m, offset, timestamp, _, err := reader.ReadMessage(ctx, headersBuf)
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
					Timestamp: timestamp,
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
