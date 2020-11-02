package server

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	client "github.com/liftbridge-io/liftbridge-api/go"
	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

const (
	defaultCursorTimeout = 5 * time.Second
	cursorCacheSize      = 512
)

// cursorManager provides an API for managing consumer cursor positions for
// stream partitions. A cursorManager can only accept operations for requests
// that map to internal cursor partitions of which this server is the leader.
// Otherwise, it will return an error.
type cursorManager struct {
	*Server
	mu           sync.RWMutex
	cache        *lru.Cache
	disableCache bool // Used for testing purposes only
}

func newCursorManager(s *Server) *cursorManager {
	// Ignoring error here because it's only returned if size is <= 0.
	cache, _ := lru.New(cursorCacheSize)
	return &cursorManager{
		Server: s,
		cache:  cache,
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
			ReplicationFactor: maxReplicationFactor,
			Id:                i,
		}
	}
	stream := &proto.Stream{
		Name:       cursorsStream,
		Subject:    c.getCursorStreamSubject(),
		Partitions: partitions,
		Config: &proto.StreamConfig{
			CompactEnabled:                &proto.NullableBool{Value: true},
			AutoPauseTime:                 &proto.NullableInt64{Value: c.config.CursorsStream.AutoPauseTime.Milliseconds()},
			AutoPauseDisableIfSubscribers: &proto.NullableBool{Value: true},
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
		cursor = &proto.Cursor{
			Stream:    streamName,
			Partition: partitionID,
			CursorId:  cursorID,
			Offset:    offset,
		}
		serializedCursor, err = cursor.Marshal()
	)
	if err != nil {
		panic(err)
	}

	ctx, cancel := ensureTimeout(ctx, defaultCursorTimeout)
	defer cancel()

	// We lock on write to ensure ordering is consistent between the partition
	// and in-memory cache even though the cache itself is thread-safe.
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err = c.api.Publish(ctx, &client.PublishRequest{
		Key:       cursorKey,
		Value:     serializedCursor,
		Stream:    cursorsStream,
		Partition: cursorsPartitionID,
		AckPolicy: client.AckPolicy_ALL,
	})
	if err != nil {
		return status.New(codes.Internal, err.Error())
	}

	// Cache the offset.
	c.cache.Add(string(cursorKey), cursor.Offset)

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

	if !c.disableCache {
		c.mu.RLock()
		if offset, ok := c.cache.Get(string(cursorKey)); ok {
			c.mu.RUnlock()
			return offset.(int64), nil
		}
		c.mu.RUnlock()
	}

	// Find the latest offset for the cursor in the log.
	offset, err := c.getLatestCursorOffset(ctx, cursorKey, cursorsPartitionID)
	if err != nil {
		return 0, status.New(codes.Internal, err.Error())
	}

	// Cache the offset.
	c.mu.Lock()
	c.cache.Add(string(cursorKey), offset)
	c.mu.Unlock()

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
	partition := c.metadata.GetPartition(cursorsStream, partitionID)
	if partition == nil {
		return 0, fmt.Errorf("Cursors partition %d does not exist", partitionID)
	}
	hw := partition.log.HighWatermark()

	// No cursors have been committed so return -1.
	if hw == -1 {
		return -1, nil
	}

	// TODO: This can likely be made more efficient.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	msgC, errC, cancel, err := c.api.SubscribeInternal(ctx, &client.SubscribeRequest{
		Stream:        cursorsStream,
		Partition:     partitionID,
		StartPosition: client.StartPosition_EARLIEST,
		Resume:        true,
	})
	if err != nil {
		return 0, err
	}
	defer cancel()
	var (
		latestOffset = int64(-1)
		cursor       = new(proto.Cursor)
	)
	for {
		select {
		case msg := <-msgC:
			if bytes.Equal(msg.Key, cursorKey) {
				if err := cursor.Unmarshal(msg.Value); err != nil {
					c.logger.Errorf("Invalid cursor message in cursors stream: %v", err)
				} else if cursor.Offset > latestOffset {
					latestOffset = cursor.Offset
				}
			}
			if msg.Offset == hw {
				return latestOffset, nil
			}
		case err := <-errC:
			return 0, err.Err()
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}
