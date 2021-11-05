package server

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

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
