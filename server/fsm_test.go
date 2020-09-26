package server

import (
	"context"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
)

// Ensure Raft FSM properly snapshots and restores state.
func TestFSMSnapshotRestore(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

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
