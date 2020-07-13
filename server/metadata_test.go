package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

// Ensure CreateStream returns an InvalidArgument error if the request doesn't
// specify any partitions.
func TestMetadataCreateStreamNoPartitions(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()
	raft := new(raftNode)
	raft.setLeader(true)
	server.setRaft(raft)

	status := metadata.CreateStream(context.Background(), &proto.CreateStreamOp{
		Stream: new(proto.Stream),
	})

	require.Equal(t, codes.InvalidArgument, status.Code())
}

// Ensure AddStream returns an error if the stream doesn't have any partitions.
func TestMetadataAddStreamNoPartitions(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	_, err := metadata.AddStream(new(proto.Stream), false)
	require.Error(t, err)
}

// Ensure AddStream returns an error if the stream already exists.
func TestMetadataAddStreamAlreadyExists(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	_, err := metadata.AddStream(&proto.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*proto.Partition{
			{
				Stream:  "foo",
				Subject: "foo",
				Id:      0,
			},
		},
	}, false)
	require.NoError(t, err)

	_, err = metadata.AddStream(&proto.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*proto.Partition{
			{
				Stream:  "foo",
				Subject: "foo",
				Id:      0,
			},
		},
	}, false)
	require.Equal(t, ErrStreamExists, err)
}

// Ensure addPartition returns an error if the partition already exists.
func TestMetadataAddPartitionAlreadyExists(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	stream, err := metadata.AddStream(&proto.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*proto.Partition{
			{
				Stream:  "foo",
				Subject: "foo",
				Id:      0,
			},
		},
	}, false)
	require.NoError(t, err)

	err = metadata.addPartition(stream, &proto.Partition{
		Stream:  "foo",
		Subject: "foo",
		Id:      0,
	}, false, nil)
	require.Error(t, err)
}

// Ensure ResumePartition returns ErrStreamNotFound if the stream doesn't
// exist.
func TestMetadataResumePartitionStreamNotFound(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	_, err := metadata.ResumePartition("foo", 0, false)
	require.Equal(t, ErrStreamNotFound, err)
}

// Ensure ResumePartition returns ErrPartitionNotFound if the partition doesn't
// exist.
func TestMetadataResumePartitionPartitionNotFound(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	_, err := metadata.AddStream(&proto.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*proto.Partition{
			{
				Stream:  "foo",
				Subject: "foo",
				Id:      0,
			},
		},
	}, false)
	require.NoError(t, err)

	_, err = metadata.ResumePartition("foo", 1, false)
	require.Equal(t, ErrPartitionNotFound, err)
}

// Ensure checkPauseStreamPreconditions returns ErrStreamNotFound if the stream
// doesn't exist.
func TestMetadataCheckPauseStreamPreconditionsStreamNotFound(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	err := metadata.checkPauseStreamPreconditions(&proto.RaftLog{
		Op:            proto.Op_PAUSE_STREAM,
		PauseStreamOp: &proto.PauseStreamOp{Stream: "foo"},
	})
	require.Equal(t, ErrStreamNotFound, err)
}

// Ensure checkResumeStreamPreconditions returns ErrStreamNotFound if the
// stream doesn't exist.
func TestMetadataCheckResumeStreamPreconditionsStreamNotFound(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	err := metadata.checkResumeStreamPreconditions(&proto.RaftLog{
		Op:             proto.Op_RESUME_STREAM,
		ResumeStreamOp: &proto.ResumeStreamOp{Stream: "foo"},
	})
	require.Equal(t, ErrStreamNotFound, err)
}

// Ensure checkResumeStreamPreconditions returns ErrPartitionNotFound if the
// partition doesn't exist.
func TestMetadataCheckResumeStreamPreconditionsPartitionNotFound(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	_, err := metadata.AddStream(&proto.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*proto.Partition{
			{
				Stream:  "foo",
				Subject: "foo",
				Id:      0,
			},
		},
	}, false)
	require.NoError(t, err)

	err = metadata.checkResumeStreamPreconditions(&proto.RaftLog{
		Op:             proto.Op_RESUME_STREAM,
		ResumeStreamOp: &proto.ResumeStreamOp{Stream: "foo", Partitions: []int32{1}},
	})
	require.Equal(t, ErrPartitionNotFound, err)
}

// Ensure partitionExists returns ErrPartitionNotFound if the partition doesn't
// exist.
func TestMetadataPartitionExistsPartitionNotFound(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	_, err := metadata.AddStream(&proto.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*proto.Partition{
			{
				Stream:  "foo",
				Subject: "foo",
				Id:      0,
			},
		},
	}, false)
	require.NoError(t, err)

	err = metadata.partitionExists("foo", 1)
	require.Equal(t, ErrPartitionNotFound, err)
}

// Ensure ensureTimeout sets the given timeout on the Context if one isn't set
// or otherwise returns the same Context.
func TestEnsureTimeout(t *testing.T) {
	ctx := context.Background()
	defaultTimeout := 5 * time.Second

	ctx, cancel := ensureTimeout(ctx, defaultTimeout)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.InDelta(t, defaultTimeout.Milliseconds(), time.Until(deadline).Milliseconds(), 1)

	timeout := 100 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ctx, cancel = ensureTimeout(ctx, defaultTimeout)
	defer cancel()

	deadline, ok = ctx.Deadline()
	require.True(t, ok)
	require.InDelta(t, timeout.Milliseconds(), time.Until(deadline).Milliseconds(), 1)
}

// Ensure checkResumeStreamPreconditions returns ErrPartitionNotFound if the
// partition doesn't exist.
func TestMetadataCheckSetStreamReadonlyPreconditionsPartitionNotFound(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	_, err := metadata.AddStream(&proto.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*proto.Partition{
			{
				Stream:  "foo",
				Subject: "foo",
				Id:      0,
			},
		},
	}, false)
	require.NoError(t, err)

	err = metadata.checkSetStreamReadonlyPreconditions(&proto.RaftLog{
		Op:                  proto.Op_SET_STREAM_READONLY,
		SetStreamReadonlyOp: &proto.SetStreamReadonlyOp{Stream: "foo", Partitions: []int32{1}},
	})
	require.Equal(t, ErrPartitionNotFound, err)
}
