package server

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/casbin/casbin/v2"
	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"github.com/stretchr/testify/require"
)

// Ensure Raft FSM properly snapshots and restores state.
func TestFSMSnapshotRestore(t *testing.T) {
	defer cleanupStorage(t)

	// Configure the server as a seed.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create some streams.
	require.NoError(t, client.CreateStream(context.Background(), "foo", "foo"))
	require.NoError(t, client.CreateStream(context.Background(), "bar", "bar", lift.Partitions(3)))

	// Force a snapshot.
	future := s1.getRaft().Snapshot()
	require.NoError(t, future.Error())

	// Restart the server.
	s1.Stop()
	s1 = runServerWithConfig(t, s1.config)
	defer s1.Stop()

	// Ensure streams are recovered.
	waitForPartition(t, 10*time.Second, "foo", 0, s1)
	waitForPartition(t, 10*time.Second, "bar", 0, s1)
	waitForPartition(t, 10*time.Second, "bar", 1, s1)
	waitForPartition(t, 10*time.Second, "bar", 2, s1)
	require.Len(t, s1.metadata.GetStreams(), 2)
}

// Ensure streams that get deleted are deleted on restart but streams that get
// deleted and then recreated do not get deleted on restart.
func TestTombstoneStreamsOnRestart(t *testing.T) {
	defer cleanupStorage(t)

	// Configure the server as a seed.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create some streams.
	require.NoError(t, client.CreateStream(context.Background(), "foo", "foo"))
	require.NoError(t, client.CreateStream(context.Background(), "bar", "bar"))

	// Delete stream foo.
	require.NoError(t, client.DeleteStream(context.Background(), "foo"))

	// Delete and then recreate stream bar.
	require.NoError(t, client.DeleteStream(context.Background(), "bar"))
	require.NoError(t, client.CreateStream(context.Background(), "bar", "bar"))

	// Publish some messages to bar.
	for i := 0; i < 2; i++ {
		_, err = client.Publish(context.Background(), "bar", []byte(strconv.Itoa(i)))
		require.NoError(t, err)
	}

	// Restart the server.
	s1.Stop()
	s1.config.Port = 5051
	s1 = runServerWithConfig(t, s1.config)
	defer s1.Stop()

	// Wait to elect self as leader.
	getMetadataLeader(t, 10*time.Second, s1)
	waitForPartition(t, 5*time.Second, "bar", 0, s1)

	client, err = lift.Connect([]string{"localhost:5051"})
	require.NoError(t, err)
	defer client.Close()

	// Ensure bar stream exists and has the expected messages.
	ch := make(chan *lift.Message)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, "bar", func(msg *lift.Message, err error) {
		require.NoError(t, err)
		ch <- msg
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		select {
		case msg := <-ch:
			require.Equal(t, []byte(strconv.Itoa(i)), msg.Value())
		case <-time.After(5 * time.Second):
			t.Fatal("Did not receive expected message")
		}
	}
	cancel()

	// Ensure foo stream doesn't exist.
	_, err = os.Stat(filepath.Join(s1Config.DataDir, "streams", "foo"))
	require.True(t, os.IsNotExist(err))
}

// TestApplyAddPolicy ensures a policy can be added via FSM.
// FSM, upon receiving a Raft log related to policy, would attempt to apply
// the Raft opertation.
func TestApplyAddPolicy(t *testing.T) {
	// Configure the server as a seed.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	getMetadataLeader(t, 10*time.Second, s1)

	// Monkey-patch authorization instance for metadata server
	policyEnforcer, err := casbin.NewEnforcer("./configs/authz/model.conf")
	require.NoError(t, err)
	s1.authzEnforcer = &authzEnforcer{enforcer: policyEnforcer}

	// Add policies via fsm
	s1.applyAddPolicy("a", "b", "read")

	// Expect policy is added
	policy := s1.authzEnforcer.enforcer.GetPolicy()
	require.Equal(t, [][]string{{"a", "b", "read"}}, policy)
}

// TestApplyAddPolicy ensures a policy can be revoked via FSM.
// FSM, upon receiving a Raft log related to policy, would attempt to apply
// the Raft opertation.
func TestApplyRevokePolicy(t *testing.T) {
	// Configure the server as a seed.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	getMetadataLeader(t, 10*time.Second, s1)

	// Monkey-patch authorization instance for metadata server
	policyEnforcer, err := casbin.NewEnforcer("./configs/authz/model.conf")
	require.NoError(t, err)

	_, err = policyEnforcer.AddPolicies([][]string{{"a", "b", "read"}, {"c", "d", "write"}})
	require.NoError(t, err)

	s1.authzEnforcer = &authzEnforcer{enforcer: policyEnforcer}

	// Revoke policy via fsm
	s1.applyRevokePolicy("a", "b", "read")

	// Expect policy is revoked
	policy := s1.authzEnforcer.enforcer.GetPolicy()
	require.Equal(t, [][]string{{"c", "d", "write"}}, policy)
}

// TestAddRevokePolicyNoFailure ensures that FSM will not throw panic or error
// when processing AddPolicy or RevokePolicy Raft log, even when the server
// does not have  authorization enabled. Note: Normally, the server should be configured
// to have Authorization enabled to use this feature. However, in case it does not, it should
// not block the Raft replication protocol.
func TestAddRevokePolicyNoFailure(t *testing.T) {
	// Configure the server as a seed.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait to elect self as leader.
	getMetadataLeader(t, 10*time.Second, s1)

	// Monkey-patch authzEnforcer to be nil
	s1.authzEnforcer = nil

	// Apply Raft log to AddPolicy
	err := s1.applyAddPolicy("a", "b", "read")
	// Expect no failure
	require.NoError(t, err)

	// Apply Raft log to RevokePolicy
	err = s1.applyRevokePolicy("a", "b", "read")
	// Expect no failure
	require.NoError(t, err)
}
