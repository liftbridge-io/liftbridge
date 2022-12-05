package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/casbin/casbin/v2"
	client "github.com/liftbridge-io/liftbridge-api/go"
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

	_, err := metadata.AddStream(new(proto.Stream), false, 0)
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
	}, false, 0)
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
	}, false, 1)
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
	}, false, 0)
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
	}, false, 0)
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
	}, false, 0)
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
	}, false, 0)
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
	}, false, 0)
	require.NoError(t, err)

	err = metadata.checkSetStreamReadonlyPreconditions(&proto.RaftLog{
		Op:                  proto.Op_SET_STREAM_READONLY,
		SetStreamReadonlyOp: &proto.SetStreamReadonlyOp{Stream: "foo", Partitions: []int32{1}},
	})
	require.Equal(t, ErrPartitionNotFound, err)
}

// TestPartitionMetadataContainOffSetandHighWaterMark ensures that High Watermark and Newest Offset are
// exposed in partition's metadata
func TestFetchPartitionMetadata(t *testing.T) {
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
	}, false, 0)
	require.NoError(t, err)

	// Monkey patch partition leader as FetchPartitionMetadata does not process
	// request if the server is not the partition's leader.
	stream := metadata.GetStream("foo")
	// Creation time should be set.
	require.True(t, time.Since(stream.GetCreationTime()) > 0)
	p := stream.GetPartition(0)
	p.isLeading = true

	req := &client.FetchPartitionMetadataRequest{
		Stream:    "foo",
		Partition: 0,
	}
	resp, status := metadata.FetchPartitionMetadata(context.Background(), req)
	require.Nil(t, status)

	// Expect high watermark, offset and timestamps are present in the metatada.

	// High watermark is -1, indicating no message on the partition.
	require.Equal(t, resp.Metadata.GetHighWatermark(), int64(-1))

	// Newest Offset is -1, indicating no message on the partition.
	require.Equal(t, resp.Metadata.GetNewestOffset(), int64(-1))

	// Expect zero timestamps.
	require.Equal(t, int64(0), resp.Metadata.MessagesReceivedTimestamps.FirstTimestamp)
	require.Equal(t, int64(0), resp.Metadata.MessagesReceivedTimestamps.LatestTimestamp)
	require.Equal(t, int64(0), resp.Metadata.PauseTimestamps.FirstTimestamp)
	require.Equal(t, int64(0), resp.Metadata.PauseTimestamps.LatestTimestamp)
	require.Equal(t, int64(0), resp.Metadata.ReadonlyTimestamps.FirstTimestamp)
	require.Equal(t, int64(0), resp.Metadata.ReadonlyTimestamps.LatestTimestamp)
}

// Ensures that FetchPartitionMetadata returns an error if the server is not
// the partition leader.
func TestFetchPartitionMetadataNotLeader(t *testing.T) {
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
	}, false, 0)
	require.NoError(t, err)

	req := &client.FetchPartitionMetadataRequest{
		Stream:    "foo",
		Partition: 0,
	}
	_, status := metadata.FetchPartitionMetadata(context.Background(), req)

	// Expect error as the server is not the partition leader.
	require.NotNil(t, status)
	require.Equal(t, codes.FailedPrecondition, status.Code())
}

// Ensures that FetchPartitionMetadata returns an error if the partition does
// not exist.
func TestFetchPartitionMetadataPartitionNotFound(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(server)
	defer metadata.Reset()

	req := &client.FetchPartitionMetadataRequest{
		Stream:    "foo",
		Partition: 0,
	}
	_, status := metadata.FetchPartitionMetadata(context.Background(), req)

	// Expect error as the stream does not exist.
	require.NotNil(t, status)
	require.Equal(t, codes.NotFound, status.Code())

}

// Ensure getPartitionReplicas selects replicas based on the amount of
// partition load they have.
func TestMetadataGetPartitionReplicas(t *testing.T) {
	defer cleanupStorage(t)

	s1 := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(s1)
	defer metadata.Reset()
	require.NoError(t, s1.Start())
	defer s1.Stop()

	s2 := New(getTestConfig("b", false, 0))
	require.NoError(t, s2.Start())
	defer s2.Stop()

	s3 := New(getTestConfig("c", false, 0))
	require.NoError(t, s3.Start())
	defer s3.Stop()

	_, err := metadata.AddStream(&proto.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*proto.Partition{
			{
				Stream:   "foo",
				Subject:  "foo",
				Id:       0,
				Replicas: []string{"a", "b"},
				Leader:   "a",
			},
			{
				Stream:   "foo",
				Subject:  "foo",
				Id:       1,
				Replicas: []string{"a", "b"},
				Leader:   "b",
			},
		},
	}, true, 0)
	require.NoError(t, err)

	_, err = metadata.AddStream(&proto.Stream{
		Name:    "bar",
		Subject: "bar",
		Partitions: []*proto.Partition{
			{
				Stream:   "bar",
				Subject:  "bar",
				Id:       0,
				Replicas: []string{"a"},
				Leader:   "a",
			},
			{
				Stream:   "bar",
				Subject:  "bar",
				Id:       1,
				Replicas: []string{"a"},
				Leader:   "a",
			},
			{
				Stream:   "bar",
				Subject:  "bar",
				Id:       2,
				Replicas: []string{"a"},
				Leader:   "a",
			},
		},
	}, true, 1)
	require.NoError(t, err)

	_, err = metadata.AddStream(&proto.Stream{
		Name:    "baz",
		Subject: "baz",
		Partitions: []*proto.Partition{
			{
				Stream:   "baz",
				Subject:  "baz",
				Id:       0,
				Replicas: []string{"a", "b", "c"},
				Leader:   "c",
			},
		},
	}, true, 2)
	require.NoError(t, err)

	// Partition load:
	// a = 6
	// b = 3
	// c = 1

	_, status := metadata.getPartitionReplicas(0)
	require.NotNil(t, status)

	replicas, status := metadata.getPartitionReplicas(-1)
	require.Nil(t, status)
	require.Equal(t, []string{"c", "b", "a"}, replicas)

	replicas, status = metadata.getPartitionReplicas(1)
	require.Nil(t, status)
	require.Equal(t, []string{"c"}, replicas)

	replicas, status = metadata.getPartitionReplicas(2)
	require.Nil(t, status)
	require.Equal(t, []string{"c", "b"}, replicas)

	replicas, status = metadata.getPartitionReplicas(3)
	require.Nil(t, status)
	require.Equal(t, []string{"c", "b", "a"}, replicas)

	_, status = metadata.getPartitionReplicas(4)
	require.NotNil(t, status)
}

// Ensure selectPartitionLeader selects the leader based on the least partition
// leadership load.
func TestMetadataSelectPartitionLeader(t *testing.T) {
	defer cleanupStorage(t)

	s1 := New(getTestConfig("a", true, 0))
	metadata := newMetadataAPI(s1)
	defer metadata.Reset()
	require.NoError(t, s1.Start())
	defer s1.Stop()

	s2 := New(getTestConfig("b", false, 0))
	require.NoError(t, s2.Start())
	defer s2.Stop()

	s3 := New(getTestConfig("c", false, 0))
	require.NoError(t, s3.Start())
	defer s3.Stop()

	_, err := metadata.AddStream(&proto.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*proto.Partition{
			{
				Stream:   "foo",
				Subject:  "foo",
				Id:       0,
				Replicas: []string{"a", "b"},
				Leader:   "a",
			},
			{
				Stream:   "foo",
				Subject:  "foo",
				Id:       1,
				Replicas: []string{"a", "b"},
				Leader:   "b",
			},
		},
	}, true, 0)
	require.NoError(t, err)

	// Partition leader load:
	// a = 1
	// b = 1
	// c = 0

	replicas := []string{"a", "b", "c"}
	leader := metadata.selectPartitionLeader(replicas)
	require.Equal(t, "c", leader)

	_, err = metadata.AddStream(&proto.Stream{
		Name:    "bar",
		Subject: "bar",
		Partitions: []*proto.Partition{
			{
				Stream:   "bar",
				Subject:  "bar",
				Id:       0,
				Replicas: []string{"b", "c"},
				Leader:   "c",
			},
			{
				Stream:   "bar",
				Subject:  "bar",
				Id:       1,
				Replicas: []string{"b", "c"},
				Leader:   "c",
			},
			{
				Stream:   "bar",
				Subject:  "bar",
				Id:       2,
				Replicas: []string{"b", "c"},
				Leader:   "b",
			},
		},
	}, true, 1)
	require.NoError(t, err)

	// Partition leader load:
	// a = 1
	// b = 2
	// c = 2

	leader = metadata.selectPartitionLeader(replicas)
	require.Equal(t, "a", leader)
}

// TestMetadataAddPolicy ensure that leader node can add ACL policies.
func TestMetadataAddPolicy(t *testing.T) {
	defer cleanupStorage(t)

	s1 := New(getTestConfig("a", true, 0))
	require.NoError(t, s1.Start())
	defer s1.Stop()

	// wait for metadata leader
	leader := getMetadataLeader(t, 10*time.Second, s1)
	require.NotNil(t, leader)

	metadata := newMetadataAPI(leader)
	defer metadata.Reset()

	// Moneky-patch authorization instance for metadata server
	policyEnforcer, err := casbin.NewEnforcer("./configs/authz/model.conf")
	require.NoError(t, err)
	metadata.Server.authzEnforcer = &authzEnforcer{enforcer: policyEnforcer}

	// AddPolicy via leader
	status := metadata.AddPolicy(context.Background(),
		&proto.AddPolicyOp{Policy: &proto.Policy{
			UserId:     "a",
			ResourceId: "b",
			Action:     "read"}})

	require.Equal(t, codes.OK, status.Code())
	s1Policies := s1.authzEnforcer.enforcer.GetPolicy()
	require.Equal(t, 1, len(s1Policies))

}

// TestMetadataAddPolicyPropagate ensures that a follower server can propagate AddPolicy request to leader
func TestMetadataAddPolicyPropagate(t *testing.T) {
	defer cleanupStorage(t)

	s1 := New(getTestConfig("a", true, 0))
	require.NoError(t, s1.Start())
	defer s1.Stop()

	s2 := New(getTestConfig("b", false, 0))
	require.NoError(t, s2.Start())
	defer s2.Stop()

	// Monkey-patch authorization instance for metadata server 1
	policyEnforcerS1, err := casbin.NewEnforcer("./configs/authz/model.conf")
	require.NoError(t, err)

	s1.authzEnforcer = &authzEnforcer{enforcer: policyEnforcerS1}

	// Monkey-patch authorization instance for metadata server 2
	policyEnforcerS2, err := casbin.NewEnforcer("./configs/authz/model.conf")
	require.NoError(t, err)

	s2.authzEnforcer = &authzEnforcer{enforcer: policyEnforcerS2}

	// wait for metadata leader
	leader := getMetadataLeader(t, 10*time.Second, s1, s2)

	var metadata = &metadataAPI{}
	if leader == s1 {
		metadata = newMetadataAPI(s2)
	} else {
		metadata = newMetadataAPI(s1)
	}
	defer metadata.Reset()

	// AddPolicy via follower
	status := metadata.AddPolicy(context.Background(),
		&proto.AddPolicyOp{Policy: &proto.Policy{
			UserId:     "a",
			ResourceId: "b",
			Action:     "read"}})
	// Expect Raft log is replicated without errors
	require.Equal(t, codes.OK, status.Code())

	// Wait until all servers fully apply RaftLog on FSM
	err = waitUntilAuthorizationLogAppliedOnFSM(t, 5*time.Second, 1, s1)
	require.NoError(t, err)
	err = waitUntilAuthorizationLogAppliedOnFSM(t, 5*time.Second, 1, s2)
	require.NoError(t, err)

	// Expect the policy is present in both leader and follower node
	expectedPolicies := []string{"a", "b", "read"}

	s1Policies := s1.authzEnforcer.enforcer.GetPolicy()
	require.Equal(t, expectedPolicies, s1Policies[0])

	s2Policies := s2.authzEnforcer.enforcer.GetPolicy()
	require.Equal(t, expectedPolicies, s2Policies[0])
}

// TestMetadataRevokePolicy ensure that leader node can remove ACL policy
func TestMetadataRevokePolicy(t *testing.T) {
	defer cleanupStorage(t)

	s1 := New(getTestConfig("a", true, 0))
	require.NoError(t, s1.Start())
	defer s1.Stop()

	// wait for metadata leader
	leader := getMetadataLeader(t, 10*time.Second, s1)
	require.NotNil(t, leader)

	// Revoke a policy via leader
	metadata := newMetadataAPI(leader)
	defer metadata.Reset()

	// Moneky-patch authorization instance for metadata server
	policyEnforcer, err := casbin.NewEnforcer("./configs/authz/model.conf")
	require.NoError(t, err)
	policyEnforcer.AddPolicy("a", "b", "read")
	metadata.Server.authzEnforcer = &authzEnforcer{enforcer: policyEnforcer}

	// Revoke the policy
	status := metadata.RevokePolicy(context.Background(),
		&proto.RevokePolicyOp{Policy: &proto.Policy{
			UserId:     "a",
			ResourceId: "b",
			Action:     "read"}})

	// Expect the policy to be successfully revoked
	require.Equal(t, codes.OK, status.Code())
	s1Policies := s1.authzEnforcer.enforcer.GetPolicy()
	require.Equal(t, 0, len(s1Policies))
}

// TestMetadataRevokePolicyPropagate ensures that a follower server can propagate RevokePolicy request to leader
func TestMetadataRevokePolicyPropagate(t *testing.T) {
	defer cleanupStorage(t)

	s1 := New(getTestConfig("a", true, 0))
	require.NoError(t, s1.Start())
	defer s1.Stop()

	s2 := New(getTestConfig("b", false, 0))
	require.NoError(t, s2.Start())
	defer s2.Stop()

	// Monkey-patch authorization instance for metadata server 1
	policyEnforcerS1, err := casbin.NewEnforcer("./configs/authz/model.conf")
	require.NoError(t, err)
	policyEnforcerS1.AddPolicy("a", "b", "read")
	s1.authzEnforcer = &authzEnforcer{enforcer: policyEnforcerS1}

	// Monkey-patch authorization instance for metadata server 2
	policyEnforcerS2, err := casbin.NewEnforcer("./configs/authz/model.conf")
	require.NoError(t, err)
	policyEnforcerS2.AddPolicy("a", "b", "read")
	s2.authzEnforcer = &authzEnforcer{enforcer: policyEnforcerS2}

	// wait for metadata leader
	leader := getMetadataLeader(t, 10*time.Second, s1, s2)

	var metadata = &metadataAPI{}
	if leader == s1 {
		metadata = newMetadataAPI(s2)
	} else {
		metadata = newMetadataAPI(s1)
	}
	defer metadata.Reset()

	// RevokePolicy via follower
	status := metadata.RevokePolicy(context.Background(),
		&proto.RevokePolicyOp{Policy: &proto.Policy{
			UserId:     "a",
			ResourceId: "b",
			Action:     "read"}})
	// Expect Raft log is replicated without errors
	require.Equal(t, codes.OK, status.Code())

	// Expect the policy is revoked in both leader and follower node
	err = waitUntilAuthorizationLogAppliedOnFSM(t, 5*time.Second, 0, s1)
	require.NoError(t, err)
	err = waitUntilAuthorizationLogAppliedOnFSM(t, 5*time.Second, 0, s2)
	require.NoError(t, err)

}

// TestMetadataListPolicy ensures that all existing ACL policies can be retrieved from metadata api.
func TestMetadataListPolicy(t *testing.T) {
	defer cleanupStorage(t)

	s1 := New(getTestConfig("a", true, 0))
	require.NoError(t, s1.Start())
	defer s1.Stop()

	// wait for metadata leader
	getMetadataLeader(t, 10*time.Second, s1)
	metadata := newMetadataAPI(s1)
	defer metadata.Reset()

	// Monkey-patch authorization instance for metadata server
	policyEnforcer, err := casbin.NewEnforcer("./configs/authz/model.conf")
	require.NoError(t, err)

	// Add policies
	_, err = policyEnforcer.AddPolicies([][]string{{"a", "b", "read"}, {"c", "d", "write"}})
	require.NoError(t, err)
	metadata.Server.authzEnforcer = &authzEnforcer{enforcer: policyEnforcer}

	// Add a policy
	status := metadata.AddPolicy(context.Background(),
		&proto.AddPolicyOp{Policy: &proto.Policy{
			UserId:     "a",
			ResourceId: "b",
			Action:     "read"}})

	require.Equal(t, codes.OK, status.Code())

	// Add a second policy
	status = metadata.AddPolicy(context.Background(),
		&proto.AddPolicyOp{Policy: &proto.Policy{
			UserId:     "c",
			ResourceId: "d",
			Action:     "write"}})

	require.Equal(t, codes.OK, status.Code())

	// Retrieve all policies
	allPoliciesResp, status := metadata.ListPolicy(context.Background(), &client.ListPolicyRequest{})
	require.Equal(t, codes.OK, status.Code())

	expectedPolicies := make(map[int32]*client.ACLPolicy)
	expectedPolicies[0] = &client.ACLPolicy{UserId: "a", ResourceId: "b", Action: "read"}
	expectedPolicies[1] = &client.ACLPolicy{UserId: "c", ResourceId: "d", Action: "write"}

	require.Equal(t, allPoliciesResp.Policies, expectedPolicies)
}

// TestRaftReplicationOnServerWithoutAuthz makes sure that no Raft replication would fail,
// even when an AddPolicy and RevokePolicy log is received on a server without authorization enabled.
// I.e: Given a server with authorization deactivated, once a Raft log for add policy and revoke policy is received,
// it will not throw panic or error. This is to ensure that RaftLog for authorization has no effect
// on the availability of the cluster
func TestRaftReplicationOnServerWithoutAuthz(t *testing.T) {
	s1 := New(getTestConfig("a", true, 0))
	require.NoError(t, s1.Start())
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	metadata := newMetadataAPI(s1)
	defer metadata.Reset()

	// Disable authorization on this server
	metadata.Server.authzEnforcer = nil

	// Add a policy
	status := metadata.AddPolicy(context.Background(),
		&proto.AddPolicyOp{Policy: &proto.Policy{
			UserId:     "a",
			ResourceId: "b",
			Action:     "read"}})

	// No effect on Raft replication
	require.Equal(t, codes.OK, status.Code())

	// Revoke policy
	status = metadata.RevokePolicy(context.Background(),
		&proto.RevokePolicyOp{Policy: &proto.Policy{
			UserId:     "a",
			ResourceId: "b",
			Action:     "read"}})

	// No effect on Raft replication
	require.Equal(t, codes.OK, status.Code())
}

// waitUntilAuthorizationLogAppliedOnFSM waits for the given server to fully apply authorization RaftLog.
// After a given time-out period, an error is returned to indicate the authorization RaftLog has not
// been fully applied on the given server's FSM.
func waitUntilAuthorizationLogAppliedOnFSM(t *testing.T, timeout time.Duration, size int, s *Server) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		authorizationPolicies := s.authzEnforcer.enforcer.GetPolicy()
		if len(authorizationPolicies) == size {
			return nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return errors.New("Authorization Raft log is not applied on the given server")
}
