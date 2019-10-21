package server

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/liftbridge-io/liftbridge-grpc/go"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type message struct {
	Key    []byte
	Value  []byte
	Offset int64
}

func assertMsg(t *testing.T, expected *message, msg *proto.Message) {
	require.Equal(t, expected.Offset, msg.Offset)
	require.Equal(t, expected.Key, msg.Key)
	require.Equal(t, expected.Value, msg.Value)
}

// Ensure creating a stream works, and it returns an error when creating the
// same stream.
func TestCreateStream(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	err = client.CreateStream(context.Background(), "foo", "bar")
	require.NoError(t, err)

	// Creating the same stream returns ErrStreamExists.
	err = client.CreateStream(context.Background(), "foo", "bar")
	require.Equal(t, lift.ErrStreamExists, err)
}

// Ensure creating a stream works when we send the request to the metadata
// follower, and it returns an error when creating the same stream.
func TestCreateStreamPropagate(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 0)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5050)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	getMetadataLeader(t, 10*time.Second, s1, s2)

	// Connect and send the request to the follower.
	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	err = client.CreateStream(context.Background(), "foo", "foo")
	require.NoError(t, err)

	// Creating the same stream returns ErrStreamExists.
	err = client.CreateStream(context.Background(), "foo", "foo")
	require.Equal(t, lift.ErrStreamExists, err)
}

// Ensure creating a stream fails when there is no known metadata leader.
func TestCreateStreamNoMetadataLeader(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 0)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5050)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Wait for a leader to be elected to allow the cluster to form, then stop
	// a server and wait for the leader to step down.
	getMetadataLeader(t, 10*time.Second, s1, s2)
	s1.Stop()
	waitForNoMetadataLeader(t, 10*time.Second, s1, s2)

	// Connect and send the request to the follower.
	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	err = client.CreateStream(context.Background(), "foo", "foo")
	require.Error(t, err)
	st := status.Convert(err)
	require.Equal(t, "No known metadata leader", st.Message())
	require.Equal(t, codes.Internal, st.Code())
}

// Ensure creating a stream fails when the replication factor is greater than
// the number of servers in the cluster.
func TestCreateStreamInsufficientReplicas(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	err = client.CreateStream(context.Background(), "foo", "foo",
		lift.ReplicationFactor(2))
	require.Error(t, err)
}

// Ensure when a partitioned stream is created the correct number of partitions
// are made.
func TestCreateStreamPartitioned(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	err = client.CreateStream(context.Background(), "foo", "foo", lift.Partitions(3))
	require.NoError(t, err)

	stream := s1.metadata.GetStream("foo")
	require.NotNil(t, stream)
	require.Len(t, stream.partitions, 3)
}

// Ensure subscribing to a non-existent stream returns an error.
func TestSubscribeStreamNoSuchStream(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	conn, err := grpc.Dial("localhost:5050", grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()
	apiClient := proto.NewAPIClient(conn)

	stream, err := apiClient.Subscribe(context.Background(),
		&proto.SubscribeRequest{Stream: "foo"})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "No such partition")
}

// Ensure sending a subscribe request to a server that is not the stream leader
// returns an error.
func TestSubscribeStreamNotLeader(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	getMetadataLeader(t, 10*time.Second, s1, s2)

	// Create the stream.
	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name,
		lift.ReplicationFactor(2))
	require.NoError(t, err)

	require.NoError(t, client.Close())

	// Wait for both nodes to create stream.
	waitForPartition(t, 5*time.Second, name, 0, s1, s2)

	// Connect to the server that is the stream follower.
	leader := getPartitionLeader(t, 10*time.Second, name, 0, s1, s2)
	var followerConfig *Config
	if leader == s1 {
		followerConfig = s2Config
	} else {
		followerConfig = s1Config
	}
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", followerConfig.Port), grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()
	apiClient := proto.NewAPIClient(conn)

	// Subscribe on the follower.
	stream, err := apiClient.Subscribe(context.Background(), &proto.SubscribeRequest{Stream: name})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "Server not partition leader")
}

// Ensure publishing and receiving messages on a stream works.
func TestStreamPublishSubscribe(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name)
	require.NoError(t, err)

	num := 5
	expected := make([]*message, num)
	for i := 0; i < num; i++ {
		expected[i] = &message{
			Key:    []byte("bar"),
			Value:  []byte(strconv.Itoa(i)),
			Offset: int64(i),
		}
	}
	i := 0
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	err = client.Subscribe(context.Background(), name, func(msg *proto.Message, err error) {
		if i == num+5 && err != nil {
			return
		}
		require.NoError(t, err)
		expect := expected[i]
		assertMsg(t, expect, msg)
		i++
		if i == num {
			close(ch1)
		}
		if i == num+5 {
			close(ch2)
		}
	})
	require.NoError(t, err)

	// Publish messages.
	for i := 0; i < num; i++ {
		_, err = client.Publish(context.Background(), subject, expected[i].Value,
			lift.Key(expected[i].Key))
		require.NoError(t, err)
	}

	// Wait to receive initial messages.
	select {
	case <-ch1:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}

	// Publish some more messages.
	for i := 0; i < 5; i++ {
		expected = append(expected, &message{
			Key:    []byte("baz"),
			Value:  []byte(strconv.Itoa(i + num)),
			Offset: int64(i + num),
		})
	}
	for i := 0; i < 5; i++ {
		_, err = client.Publish(context.Background(), subject, expected[i+num].Value,
			lift.Key(expected[i+num].Key))
		require.NoError(t, err)
	}

	// Wait to receive remaining messages.
	select {
	case <-ch2:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}

	// Make sure we can play back the log.
	client2, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client2.Close()
	i = num
	ch1 = make(chan struct{})
	err = client2.Subscribe(context.Background(), name,
		func(msg *proto.Message, err error) {
			if i == num+5 && err != nil {
				return
			}
			require.NoError(t, err)
			expect := expected[i]
			assertMsg(t, expect, msg)
			i++
			if i == num+5 {
				close(ch1)
			}
		}, lift.StartAtOffset(int64(num)))
	require.NoError(t, err)

	select {
	case <-ch1:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}
}
