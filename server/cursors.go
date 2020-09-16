package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

// cursorManager provides an API for managing consumer cursor positions for
// stream partitions.
type cursorManager struct {
	*Server
	mu sync.RWMutex
}

func newCursorManager(s *Server) *cursorManager {
	return &cursorManager{
		Server: s,
	}
}

// Initialize cursor management by creating the cursors stream if it doesn't
// yet exist and the configured number of partitions is greater than 0. This
// should be called when this node has been elected metadata leader to ensure
// the cursors stream exists.
func (c *cursorManager) Initialize() error {
	if c.config.CursorsStream.Partitions == 0 {
		return nil
	}
	if stream := c.metadata.GetStream(cursorsStream); stream != nil {
		return nil
	}

	partitions := make([]*proto.Partition, c.config.CursorsStream.Partitions)
	for i := int32(0); i < c.config.CursorsStream.Partitions; i++ {
		partitions[i] = &proto.Partition{
			Subject:           c.getCursorStreamSubject(),
			Stream:            cursorsStream,
			ReplicationFactor: maxReplicationFactor, // TODO: Make configurable
			Id:                i,
		}
	}
	stream := &proto.Stream{
		Name:       cursorsStream,
		Subject:    c.getCursorStreamSubject(),
		Partitions: partitions,
		Config: &proto.StreamConfig{
			CompactEnabled: &proto.NullableBool{Value: true},
		},
	}
	status := c.metadata.CreateStream(context.Background(), &proto.CreateStreamOp{Stream: stream})
	if status == nil || status.Code() == codes.AlreadyExists {
		return nil
	}

	return status.Err()
}

// SetCursor stores a cursor position for a particular stream partition
// uniquely identified by an opaque string. This returns an error if persisting
// the cursor failed.
func (c *cursorManager) SetCursor(ctx context.Context, streamName, cursorID string, partitionID int32, offset int64) *status.Status {
	var (
		cursorKey              = c.getCursorKey(cursorID, streamName, partitionID)
		cursorsPartitionID, st = c.getCursorsPartitionID(cursorKey)
	)
	if st != nil {
		return st
	}

	var (
		cursor, err = (&proto.Cursor{
			Stream:    streamName,
			Partition: partitionID,
			CursorId:  cursorID,
			Offset:    offset,
		}).Marshal()
	)
	if err != nil {
		panic(err)
	}

	_, err = c.getLoopbackClient().Publish(ctx, cursorsStream, cursor,
		lift.ToPartition(cursorsPartitionID), lift.Key(cursorKey), lift.AckPolicyAll())
	if err != nil {
		return status.New(codes.Internal, err.Error())
	}

	// TODO: Add caching

	return nil
}

// GetCursor returns the latest partition offset for the given cursor, if it
// exists.
func (c *cursorManager) GetCursor(ctx context.Context, streamName, cursorID string, partitionID int32) (int64, *status.Status) {
	var (
		cursorKey              = c.getCursorKey(cursorID, streamName, partitionID)
		cursorsPartitionID, st = c.getCursorsPartitionID(cursorKey)
	)
	if st != nil {
		return 0, st
	}
	offset, err := c.getLatestCursorOffset(ctx, cursorKey, cursorsPartitionID)
	if err != nil {
		return 0, status.New(codes.Internal, err.Error())
	}
	return offset, nil
}

func (c *cursorManager) getCursorsPartitionID(cursorKey []byte) (int32, *status.Status) {
	stream := c.metadata.GetStream(cursorsStream)
	if stream == nil {
		return 0, status.New(codes.Internal, "Cursors stream does not exist")
	}

	var (
		cursorsPartitionID = int32(hasher(cursorKey) % uint32(len(stream.GetPartitions())))
		cursorsPartition   = stream.GetPartition(cursorsPartitionID)
	)
	if cursorsPartition == nil {
		return 0, status.Newf(codes.Internal, "Cursors partition %d does not exist", cursorsPartitionID)
	}

	leader, _ := cursorsPartition.GetLeader()
	if leader != c.config.Clustering.ServerID {
		// TODO: Attempt to forward to partition leader.
		return 0, status.Newf(codes.FailedPrecondition, "Server not leader for cursors partition %d", cursorsPartitionID)
	}

	return cursorsPartitionID, nil
}

func (c *cursorManager) getCursorKey(cursorID, streamName string, partitionID int32) []byte {
	return []byte(fmt.Sprintf("%s,%s,%d", cursorID, streamName, partitionID))
}

func (c *cursorManager) getLatestCursorOffset(ctx context.Context, cursorKey []byte, partitionID int32) (int64, error) {
	// TODO: Add caching

	partition := c.metadata.GetPartition(cursorsStream, partitionID)
	if partition == nil {
		return 0, fmt.Errorf("Cursors partition %d does not exist", partitionID)
	}
	hw := partition.log.HighWatermark()

	// No cursors have been committed so return -1.
	if hw == -1 {
		return -1, nil
	}

	// Find the latest offset for the cursor in the log.
	// TODO: This can likely be made more efficient.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		errC = make(chan error, 1)
		msgC = make(chan *lift.Message, 1)
	)
	c.getLoopbackClient().Subscribe(ctx, cursorsStream, func(msg *lift.Message, err error) {
		defer func() {
			if msg.Offset() == hw {
				errC <- io.EOF
			}
		}()
		if err != nil {
			errC <- err
			return
		}
		if !bytes.Equal(msg.Key(), cursorKey) {
			return
		}
		msgC <- msg

	}, lift.Partition(partitionID), lift.StartAtEarliestReceived())

	var (
		latestOffset = int64(-1)
		cursor       = new(proto.Cursor)
	)
	for {
		select {
		case msg := <-msgC:
			if err := cursor.Unmarshal(msg.Value()); err != nil {
				c.logger.Errorf("Invalid cursor message in cursors stream: %v", err)
				continue
			}
			if cursor.Offset > latestOffset {
				latestOffset = cursor.Offset
			}
		case err := <-errC:
			return 0, err
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

}
