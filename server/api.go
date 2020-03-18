package server

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	client "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/liftbridge-io/liftbridge/server/commitlog"
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

	if req.Name == "" {
		a.logger.Errorf("api: Failed to create stream: name cannot be empty")
		return nil, status.Error(codes.InvalidArgument, "Name cannot be empty")
	}
	// TODO: Check if valid NATS subject?
	if req.Subject == "" {
		a.logger.Errorf("api: Failed to create stream: subject cannot be empty")
		return nil, status.Error(codes.InvalidArgument, "Subject cannot be empty")
	}

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

// DeleteStream deletes a stream attached to a NATS subject.
func (a *apiServer) DeleteStream(ctx context.Context, req *client.DeleteStreamRequest) (
	*client.DeleteStreamResponse, error) {

	resp := &client.DeleteStreamResponse{}
	a.logger.Debugf("api: DeleteStream [name=%s]",
		req.Name)

	if e := a.metadata.DeleteStream(ctx, &proto.DeleteStreamOp{
		Stream: req.Name,
	}); e != nil {
		a.logger.Errorf("api: Failed to delete stream %v: %v", req.Name, e.Err())
		return nil, e.Err()
	}

	return resp, nil
}

// PauseStream pauses a stream attached to a NATS subject.
func (a *apiServer) PauseStream(ctx context.Context, req *client.PauseStreamRequest) (
	*client.PauseStreamResponse, error) {

	resp := &client.PauseStreamResponse{}
	a.logger.Debugf("api: PauseStream [name=%s]",
		req.Name)

	if e := a.metadata.PauseStream(ctx, &proto.PauseStreamOp{
		Stream: req.Name,
	}); e != nil {
		a.logger.Errorf("api: Failed to pause stream %v: %v", req.Name, e.Err())
		return nil, e.Err()
	}

	return resp, nil
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
	subject, err := a.getPublishSubject(req)
	if err != nil {
		return nil, err
	}
	a.logger.Debugf("api: Publish [subject=%s]", subject)

	stream := a.metadata.GetStream(req.Stream)
	if stream == nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("No such stream: %s", req.Stream))
	}
	partition, ok := stream.partitions[0] // Only check the first partition for now
	if !ok {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("No partition in stream: %s", req.Stream))
	}
	if partition.paused {
		// This partition (and thus, this stream) is paused.
		// We need to re-create the stream before being able to use it.
		protoPart := partition.Partition
		for i := 0; i < len(stream.partitions); i++ {
			if e := a.metadata.CreatePartition(ctx, &proto.CreatePartitionOp{
				Partition: protoPart,
			}); e != nil {
				a.logger.Errorf("api: Failed to resume stream partition %d: %v", i, e.Err())
				return nil, e.Err()
			}
		}
	}

	if req.AckInbox == "" {
		req.AckInbox = nuid.Next()
	}

	msg := &client.Message{
		Key:           req.Key,
		Value:         req.Value,
		Stream:        req.Stream,
		Subject:       subject,
		ReplySubject:  req.ReplySubject,
		Headers:       req.Headers,
		AckInbox:      req.AckInbox,
		CorrelationId: req.CorrelationId,
		AckPolicy:     req.AckPolicy,
	}

	buf, err := proto.MarshalPublish(msg)
	if err != nil {
		a.logger.Errorf("api: Failed to marshal message: %v", err.Error())
		return nil, err
	}

	// If AckPolicy is NONE or a timeout isn't specified, then we will fire and
	// forget.
	var (
		resp           = new(client.PublishResponse)
		_, hasDeadline = ctx.Deadline()
	)
	if req.AckPolicy == client.AckPolicy_NONE || !hasDeadline {
		if err := a.ncPublishes.Publish(subject, buf); err != nil {
			a.logger.Errorf("api: Failed to publish message: %v", err)
			return nil, err
		}
		return resp, nil
	}

	// Otherwise we need to publish and wait for the ack.
	resp.Ack, err = a.publishSync(ctx, subject, req.AckInbox, buf)
	return resp, err
}

func (a *apiServer) getPublishSubject(req *client.PublishRequest) (string, error) {
	if req.Subject != "" {
		return req.Subject, nil
	}
	if req.Stream == "" {
		return "", status.Error(codes.InvalidArgument, "No stream or subject provided")
	}
	stream := a.metadata.GetStream(req.Stream)
	if stream == nil {
		return "", status.Error(codes.NotFound, fmt.Sprintf("No such stream: %s", req.Stream))
	}
	subject := stream.subject
	if req.Partition > 0 {
		subject = fmt.Sprintf("%s.%d", subject, req.Partition)
	}
	return subject, nil
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

	ack, err := proto.UnmarshalAck(ackMsg.Data)
	if err != nil {
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

	startOffset, st := getStartOffset(req, partition.log)
	if st != nil {
		return nil, nil, st
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
				var s *status.Status
				if err == commitlog.ErrCommitLogDeleted {
					s = status.New(codes.NotFound, err.Error())
				} else {
					s = status.Convert(err)
				}

				select {
				case errCh <- s:
				case <-cancel:
				}
				return
			}
			headers := m.Headers()
			var (
				msg = &client.Message{
					Stream:       partition.Stream,
					Partition:    partition.Id,
					Offset:       offset,
					Key:          m.Key(),
					Value:        m.Value(),
					Timestamp:    timestamp,
					Headers:      headers,
					Subject:      string(headers["subject"]),
					ReplySubject: string(headers["reply"]),
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

func getStartOffset(req *client.SubscribeRequest, log commitlog.CommitLog) (int64, *status.Status) {
	var startOffset int64
	switch req.StartPosition {
	case client.StartPosition_OFFSET:
		startOffset = req.StartOffset
	case client.StartPosition_TIMESTAMP:
		offset, err := log.OffsetForTimestamp(req.StartTimestamp)
		if err != nil {
			return startOffset, status.New(
				codes.Internal, fmt.Sprintf("Failed to lookup offset for timestamp: %v", err))
		}
		startOffset = offset
	case client.StartPosition_EARLIEST:
		startOffset = log.OldestOffset()
	case client.StartPosition_LATEST:
		startOffset = log.NewestOffset()
	case client.StartPosition_NEW_ONLY:
		startOffset = log.NewestOffset() + 1
	default:
		return startOffset, status.New(
			codes.InvalidArgument,
			fmt.Sprintf("Unknown StartPosition %s", req.StartPosition))
	}

	// If log is empty, next offset will be 0.
	if startOffset < 0 {
		startOffset = 0
	}

	return startOffset, nil
}
