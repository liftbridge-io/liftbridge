package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"

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
func (c *cursorManager) SetCursor(streamName string, partitionID int32, cursorID string, offset int64) error {
	stream := c.metadata.GetStream(cursorsStream)
	if stream == nil {
		return errors.New("Cursors stream does not exist")
	}

	var (
		cursorsPartitionID = int32(hasher([]byte(cursorID)) % uint32(len(stream.GetPartitions())))
		cursorsPartition   = stream.GetPartition(cursorsPartitionID)
	)
	if cursorsPartition == nil {
		return fmt.Errorf("Cursors partition %d does not exist", cursorsPartitionID)
	}

	leader, _ := cursorsPartition.GetLeader()
	if leader != c.config.Clustering.ServerID {
		// TODO: Attempt to forward to partition leader.
		return fmt.Errorf("Server not leader for cursors partition %d", cursorsPartitionID)
	}

	// TODO
	return nil
}
