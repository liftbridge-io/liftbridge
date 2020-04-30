package server

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	proto "github.com/liftbridge-io/liftbridge-api/go"

	"github.com/liftbridge-io/liftbridge/server/protocol"
)

type message struct {
	Key    []byte
	Value  []byte
	Offset int64
}

func assertMsg(t *testing.T, expected *message, msg *lift.Message) {
	require.Equal(t, expected.Offset, msg.Offset())
	require.Equal(t, expected.Key, msg.Key())
	require.Equal(t, expected.Value, msg.Value())
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

	// Connect and send the request to the follower.
	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = client.CreateStream(ctx, "foo", "foo")
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
	waitForNoMetadataLeader(t, 10*time.Second, s2)

	// Connect and send the request to the follower.
	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	err = client.CreateStream(context.Background(), "foo", "foo")
	require.Error(t, err)
	st := status.Convert(err)
	require.Equal(t, "no known metadata leader", st.Message())
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

// Ensure getting a deleted stream returns nil.
func TestDeleteStream(t *testing.T) {
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

	err = client.DeleteStream(context.Background(), "foo")
	require.Error(t, err)

	err = client.CreateStream(context.Background(), "foo", "foo", lift.Partitions(3))
	require.NoError(t, err)

	stream := s1.metadata.GetStream("foo")
	require.NotNil(t, stream)

	err = client.DeleteStream(context.Background(), "foo")
	require.NoError(t, err)

	stream = s1.metadata.GetStream("foo")
	require.Nil(t, stream)

	err = client.CreateStream(context.Background(), "foo", "foo", lift.Partitions(3))
	require.NoError(t, err)
}

// Ensure deleting a stream works when we send the request to the metadata
// follower.
func TestDeleteStreamPropagate(t *testing.T) {
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

	// Connect and send the request to the follower.
	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = client.CreateStream(ctx, "foo", "foo")
	require.NoError(t, err)

	err = client.DeleteStream(context.Background(), "foo")
	require.NoError(t, err)
}

// Ensure sending a subscribe request to a server that is not the stream leader
// with an explication opt-in to requestion from a random ISR replica
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

	followerAdd := fmt.Sprintf("localhost:%d", followerConfig.Port)
	client2, err := lift.Connect([]string{followerAdd})
	require.NoError(t, err)
	defer client2.Close()

	// Subscribe on the follower.
	err = client2.Subscribe(context.Background(), name,
		func(msg *lift.Message, err error) {
			require.NoError(t, err)
			fmt.Println("receiving msg")
		}, lift.ReadISRReplica())
	require.NoError(t, err)
}

// Ensure sending a subscribe request to a server that is not the stream leader
// returns an error. By default, do not take subscription to stream's replica.
func TestSubscribeStreamNotLeaderDefaultBehavior(t *testing.T) {
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

	// Subscribe directly on the follower without opt-in option ReadReplica.
	stream, err := apiClient.Subscribe(context.Background(), &proto.SubscribeRequest{Stream: name})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "Server not partition leader")

}

// Ensure publishing and receiving messages on a stream works,
// given that the subscription request was binded to a random ISR
func TestStreamReceiveMsgFromReplica(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()
	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	getMetadataLeader(t, 10*time.Second, s1, s2)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name, lift.ReplicationFactor(2))
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

	leader := getPartitionLeader(t, 10*time.Second, name, 0, s1, s2)
	var followerConfig *Config
	if leader == s1 {
		followerConfig = s2Config
	} else {
		followerConfig = s1Config
	}

	followerAdd := fmt.Sprintf("localhost:%d", followerConfig.Port)
	client2, err := lift.Connect([]string{followerAdd})
	require.NoError(t, err)
	defer client2.Close()

	// Subscribe on the follower.
	err = client2.Subscribe(context.Background(), name, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		//expect := expected[i]
		//assertMsg(t, expect, msg)
		i++
		if i == num {
			close(ch1)
		}
		if i == num+5 {
			close(ch2)
		}
	}, lift.ReadISRReplica())
	require.NoError(t, err)

	// Publish messages.
	for i := 0; i < num; i++ {
		_, err = client.Publish(context.Background(), name, expected[i].Value,
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
		_, err = client.Publish(context.Background(), name, expected[i+num].Value,
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
	client3, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client3.Close()
	i = num
	ch1 = make(chan struct{})
	err = client3.Subscribe(context.Background(), name,
		func(msg *lift.Message, err error) {
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
	err = client.Subscribe(context.Background(), name, func(msg *lift.Message, err error) {
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
		_, err = client.Publish(context.Background(), name, expected[i].Value,
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
		_, err = client.Publish(context.Background(), name, expected[i+num].Value,
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
		func(msg *lift.Message, err error) {
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

// Ensure publishing to a NATS subject works.
func TestPublishToSubject(t *testing.T) {
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

	// Create NATS connection.
	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync("foo.bar")
	require.NoError(t, err)

	// Publish message to subject.
	_, err = client.PublishToSubject(context.Background(), "foo.bar", []byte("hello"),
		lift.Key([]byte("key")))
	require.NoError(t, err)

	msg, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	liftMsg, err := lift.UnmarshalMessage(msg.Data)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), liftMsg.Value())
	require.Equal(t, []byte("key"), liftMsg.Key())
}

// Ensure subscribe sends a NotFound error when the partition is deleted.
func TestSubscribePartitionDeleted(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	api := &apiServer{server}
	streamProto := &protocol.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*protocol.Partition{
			{
				Stream: "foo",
				Id:     0,
			},
		},
	}
	stream, err := server.metadata.AddStream(streamProto, true)
	require.NoError(t, err)

	req := &proto.SubscribeRequest{StartPosition: proto.StartPosition_NEW_ONLY}
	_, statusCh, status := api.subscribe(context.Background(), stream.GetPartitions()[0], req, make(chan struct{}))
	require.Nil(t, status)

	require.NoError(t, stream.Delete())

	select {
	case status := <-statusCh:
		require.Equal(t, codes.NotFound, status.Code())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected status")
	}
}

// Ensure subscribe sends a FailedPrecondition error when the partition is
// paused.
func TestSubscribePartitionPaused(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	api := &apiServer{server}
	streamProto := &protocol.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*protocol.Partition{
			{
				Stream: "foo",
				Id:     0,
			},
		},
	}
	stream, err := server.metadata.AddStream(streamProto, true)
	require.NoError(t, err)

	req := &proto.SubscribeRequest{StartPosition: proto.StartPosition_NEW_ONLY}
	_, statusCh, status := api.subscribe(context.Background(), stream.GetPartitions()[0], req, make(chan struct{}))
	require.Nil(t, status)

	require.NoError(t, stream.Pause(nil, true))

	select {
	case status := <-statusCh:
		require.Equal(t, codes.FailedPrecondition, status.Code())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected status")
	}
}

// Ensure subscribe sends an Internal error when the partition is closed.
func TestSubscribePartitionClosed(t *testing.T) {
	defer cleanupStorage(t)

	server := New(getTestConfig("a", true, 0))
	api := &apiServer{server}
	streamProto := &protocol.Stream{
		Name:    "foo",
		Subject: "foo",
		Partitions: []*protocol.Partition{
			{
				Stream: "foo",
				Id:     0,
			},
		},
	}
	stream, err := server.metadata.AddStream(streamProto, true)
	require.NoError(t, err)

	req := &proto.SubscribeRequest{StartPosition: proto.StartPosition_NEW_ONLY}
	_, statusCh, status := api.subscribe(context.Background(), stream.GetPartitions()[0], req, make(chan struct{}))
	require.Nil(t, status)

	require.NoError(t, stream.Close())

	select {
	case status := <-statusCh:
		require.Equal(t, codes.Internal, status.Code())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected status")
	}
}
