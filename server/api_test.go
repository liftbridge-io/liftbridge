package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
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

	_, err = os.Stat(filepath.Join(s1Config.DataDir, "streams", "foo"))
	require.NoError(t, err)

	stream := s1.metadata.GetStream("foo")
	require.NotNil(t, stream)

	err = client.DeleteStream(context.Background(), "foo")
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(s1Config.DataDir, "streams", "foo"))
	require.True(t, os.IsNotExist(err))

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

// Ensure legacy Publish endpoint works.
func TestLegacyPublish(t *testing.T) {
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
	err = client.CreateStream(context.Background(), subject, name, lift.Partitions(2))
	require.NoError(t, err)

	conn, err := grpc.Dial("localhost:5050", grpc.WithInsecure())
	require.NoError(t, err)
	apiClient := proto.NewAPIClient(conn)

	// Publishing with no stream returns an error.
	_, err = apiClient.Publish(context.Background(), &proto.PublishRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Convert(err).Code())

	// Publishing to non-existant stream returns an error.
	_, err = apiClient.Publish(context.Background(), &proto.PublishRequest{
		Stream: "bar",
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Convert(err).Code())

	// Successful publish returns an ack when AckPolicy is not NONE and a
	// timeout is set.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := apiClient.Publish(ctx, &proto.PublishRequest{
		Stream:    "foo",
		Partition: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Ack)

	// Successful publish returns nil when AckPolicy is NONE.
	resp, err = apiClient.Publish(context.Background(), &proto.PublishRequest{
		Stream:    "foo",
		Partition: 1,
		AckPolicy: proto.AckPolicy_NONE,
	})
	require.NoError(t, err)
	require.Nil(t, resp.Ack)
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
		lift.Key([]byte("key")), lift.AckPolicyNone())
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

func TestSubscribeStopPosition(t *testing.T) {
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

	stream := "foo"
	ctx := context.Background()
	err = client.CreateStream(ctx, "foo", stream)
	require.NoError(t, err)

	// subscription to empty stream errors
	err = client.Subscribe(ctx, stream, nil, lift.StopAtLatestReceived())
	require.Error(t, err)
	require.Equal(t, lift.ErrReadonlyPartition, err)

	// publish messages to stream
	numMessages := 10
	acks := make([]*lift.Ack, 0, numMessages)
	for i := 0; i < numMessages; i++ {
		ack, err := client.Publish(ctx, stream, []byte(strconv.Itoa(i)))
		require.NoError(t, err)
		acks = append(acks, ack)
	}

	exhausted := make(chan struct{})

	// stream should return all message then close
	receivedCount := 0
	err = client.Subscribe(ctx, stream, func(msg *lift.Message, err error) {
		receivedCount++

		// we've recieved all messages so this must be a resource exhaused error
		if receivedCount > numMessages {
			require.Error(t, err)
			require.Equal(t, lift.ErrReadonlyPartition, err)
			exhausted <- struct{}{}
		}
	},
		lift.StartAtEarliestReceived(),
		lift.StopAtLatestReceived(),
	)
	require.NoError(t, err)

	select {
	case <-exhausted:
	case <-time.After(5 * time.Second):
		t.Fatal("Stream was not exhausted")
	}

	// Get all messages between the timestamps of two already acknowledged
	// messages. Inclusive of the bounds:
	// 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
	//      |_____________|
	start := acks[2]
	stop := acks[6]
	expectedCount := 5
	expectedOffset := start.Offset()

	receivedCount = 0
	err = client.Subscribe(ctx, stream, func(msg *lift.Message, err error) {
		receivedCount++

		if msg != nil {
			require.Equal(t, expectedOffset, msg.Offset())
			expectedOffset++
		}

		// we've recieved all messages so this must be a resource exhaused error
		if receivedCount > expectedCount {
			require.Error(t, err)
			require.Equal(t, lift.ErrReadonlyPartition, err)
			exhausted <- struct{}{}
		}
	},
		lift.StartAtTime(start.ReceptionTimestamp()),
		lift.StopAtTime(stop.ReceptionTimestamp()),
	)
	require.NoError(t, err)

	select {
	case <-exhausted:
	case <-time.After(5 * time.Second):
		t.Fatal("Stream was not exhausted")
	}
}

// Ensure getStreamConfig applies non-nil values from the CreateStreamRequest
// to the StreamConfig.
func TestGetStreamConfig(t *testing.T) {
	req := &proto.CreateStreamRequest{
		RetentionMaxAge:               &proto.NullableInt64{Value: 1},
		CleanerInterval:               &proto.NullableInt64{Value: 2},
		SegmentMaxBytes:               &proto.NullableInt64{Value: 3},
		SegmentMaxAge:                 &proto.NullableInt64{Value: 4},
		CompactMaxGoroutines:          &proto.NullableInt32{Value: 5},
		RetentionMaxBytes:             &proto.NullableInt64{Value: 6},
		RetentionMaxMessages:          &proto.NullableInt64{Value: 7},
		CompactEnabled:                &proto.NullableBool{Value: true},
		AutoPauseTime:                 &proto.NullableInt64{Value: 8},
		AutoPauseDisableIfSubscribers: &proto.NullableBool{Value: true},
		MinIsr:                        &proto.NullableInt32{Value: 9},
	}

	config := getStreamConfig(req)

	require.Equal(t, int64(1), config.RetentionMaxAge.Value)
	require.Equal(t, int64(2), config.CleanerInterval.Value)
	require.Equal(t, int64(3), config.SegmentMaxBytes.Value)
	require.Equal(t, int64(4), config.SegmentMaxAge.Value)
	require.Equal(t, int32(5), config.CompactMaxGoroutines.Value)
	require.Equal(t, int64(6), config.RetentionMaxBytes.Value)
	require.Equal(t, int64(7), config.RetentionMaxMessages.Value)
	require.True(t, config.CompactEnabled.Value)
	require.Equal(t, int64(8), config.AutoPauseTime.Value)
	require.True(t, config.AutoPauseDisableIfSubscribers.Value)
	require.Equal(t, int32(9), config.MinIsr.Value)
}

// Ensure SetCursor stores cursors and FetchCursor retrieves them.
func TestSetFetchCursor(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.CursorsStream.Partitions = 5
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	stream := "foo"
	partition := int32(2)
	cursorID := "abc"

	err = client.CreateStream(context.Background(), "foo", stream, lift.Partitions(5))
	require.NoError(t, err)

	// Fetching cursor that doesn't exist returns -1.
	offset, err := client.FetchCursor(context.Background(), cursorID, stream, partition)
	require.NoError(t, err)
	require.Equal(t, int64(-1), offset)

	err = client.SetCursor(context.Background(), "foo", stream, partition, 2)
	require.NoError(t, err)
	err = client.SetCursor(context.Background(), cursorID, stream, partition, 5)
	require.NoError(t, err)

	offset, err = client.FetchCursor(context.Background(), cursorID, stream, partition)
	require.NoError(t, err)
	require.Equal(t, int64(5), offset)

	err = client.SetCursor(context.Background(), cursorID, stream, partition, 10)
	require.NoError(t, err)

	offset, err = client.FetchCursor(context.Background(), cursorID, stream, partition)
	require.NoError(t, err)
	require.Equal(t, int64(10), offset)

	require.Error(t, client.SetCursor(context.Background(), "", "foo", 0, 5))
	require.Error(t, client.SetCursor(context.Background(), "123", "", 0, 5))

	_, err = client.FetchCursor(context.Background(), "", "foo", 0)
	require.Error(t, err)
	_, err = client.FetchCursor(context.Background(), "123", "", 0)
	require.Error(t, err)
}

// Ensure SetCursor stores cursors and FetchCursor retrieves them even if the
// cache is empty.
func TestSetFetchCursorNoCache(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.CursorsStream.Partitions = 5
	s1 := runServerWithConfig(t, s1Config)
	s1.cursors.disableCache = true
	defer s1.Stop()

	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	stream := "foo"
	partition := int32(2)
	cursorID := "abc"

	err = client.CreateStream(context.Background(), "foo", stream, lift.Partitions(5))
	require.NoError(t, err)

	err = client.SetCursor(context.Background(), "foo", stream, partition, 2)
	require.NoError(t, err)
	err = client.SetCursor(context.Background(), cursorID, stream, partition, 5)
	require.NoError(t, err)

	offset, err := client.FetchCursor(context.Background(), cursorID, stream, partition)
	require.NoError(t, err)
	require.Equal(t, int64(5), offset)

	err = client.SetCursor(context.Background(), cursorID, stream, partition, 10)
	require.NoError(t, err)

	offset, err = client.FetchCursor(context.Background(), cursorID, stream, partition)
	require.NoError(t, err)
	require.Equal(t, int64(10), offset)

	require.Error(t, client.SetCursor(context.Background(), "", "foo", 0, 5))
	require.Error(t, client.SetCursor(context.Background(), "123", "", 0, 5))

	_, err = client.FetchCursor(context.Background(), "", "foo", 0)
	require.Error(t, err)
	_, err = client.FetchCursor(context.Background(), "123", "", 0)
	require.Error(t, err)
}

// publishAndReceive publishes and waits for a message to arrive.
func publishAndReceive(t *testing.T, client lift.Client, stream string) {
	gotMsg := make(chan struct{})

	// Subscribe to receive one message.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := client.Subscribe(context.Background(), stream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		gotMsg <- struct{}{}
		cancel()
	})
	require.NoError(t, err)

	// Publish one message.
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Publish(ctx, stream, []byte{}, lift.AckPolicyLeader())
	require.NoError(t, err)

	select {
	case <-gotMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensures the message received timestamps returned by FetchPartitionMetadata
// are updated with coherent values.
func TestFetchPartitionMetadataMessagesReceivedTimestamps(t *testing.T) {
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

	stream := "foo"

	err = client.CreateStream(context.Background(), "foo", stream)
	require.NoError(t, err)

	metadata, err := client.FetchPartitionMetadata(context.Background(), stream, 0)
	require.NoError(t, err)

	// Check that the timestamps are zero.
	require.True(t, metadata.MessagesReceivedTimestamps().FirstTime().IsZero())
	require.True(t, metadata.MessagesReceivedTimestamps().LatestTime().IsZero())

	// Publish a first message.
	publishAndReceive(t, client, stream)

	metadata, err = client.FetchPartitionMetadata(context.Background(), stream, 0)
	require.NoError(t, err)

	// Check that the first messages received timestamp has been updated, and
	// that the latest timestamp has the same value.
	firstMessagesReceivedTimestamp := metadata.MessagesReceivedTimestamps().FirstTime()
	require.False(t, firstMessagesReceivedTimestamp.IsZero())
	require.Equal(t, firstMessagesReceivedTimestamp, metadata.MessagesReceivedTimestamps().LatestTime())

	// Publish a second message.
	publishAndReceive(t, client, stream)

	metadata, err = client.FetchPartitionMetadata(context.Background(), stream, 0)
	require.NoError(t, err)

	// Check that the first messages received timestamp still has the same value
	// and that the latest timestamp has been updated.
	require.Equal(t, firstMessagesReceivedTimestamp, metadata.MessagesReceivedTimestamps().FirstTime())
	require.True(t, metadata.MessagesReceivedTimestamps().LatestTime().After(firstMessagesReceivedTimestamp))
}

// Ensures the pause timestamps returned by FetchPartitionMetadata are updated
// with coherent values.
func TestFetchPartitionMetadataPauseTimestamps(t *testing.T) {
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

	stream := "foo"

	err = client.CreateStream(context.Background(), "foo", stream)
	require.NoError(t, err)

	metadata, err := client.FetchPartitionMetadata(context.Background(), stream, 0)
	require.NoError(t, err)

	// Check that the timestamps are zero.
	require.True(t, metadata.PauseTimestamps().FirstTime().IsZero())
	require.True(t, metadata.PauseTimestamps().LatestTime().IsZero())

	// Pause the stream.
	err = client.PauseStream(context.Background(), stream)
	require.NoError(t, err)

	// Check that FetchPartitionMetadata fails on a paused stream.
	_, err = client.FetchPartitionMetadata(context.Background(), stream, 0)
	require.Error(t, err)

	// Publish a first message, unpausing the stream.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Publish(ctx, stream, []byte{}, lift.AckPolicyLeader())
	require.NoError(t, err)

	metadata, err = client.FetchPartitionMetadata(context.Background(), stream, 0)
	require.NoError(t, err)

	// Check that the first pause timestamp has been set, and that the latest
	// timestamp is set after it.
	firstPauseTimestamp := metadata.PauseTimestamps().FirstTime()
	require.False(t, firstPauseTimestamp.IsZero())
	require.True(t, metadata.PauseTimestamps().LatestTime().After(firstPauseTimestamp))
}

// Ensures the readonly timestamps returned by FetchPartitionMetadata are
// updated with coherent values.
func TestFetchPartitionMetadataReadonlyTimestamps(t *testing.T) {
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

	stream := "foo"

	err = client.CreateStream(context.Background(), "foo", stream)
	require.NoError(t, err)

	metadata, err := client.FetchPartitionMetadata(context.Background(), stream, 0)
	require.NoError(t, err)

	// Check that the timestamps are zero.
	require.True(t, metadata.ReadonlyTimestamps().FirstTime().IsZero())
	require.True(t, metadata.ReadonlyTimestamps().LatestTime().IsZero())

	// Set the stream as readonly.
	err = client.SetStreamReadonly(context.Background(), stream)
	require.NoError(t, err)

	metadata, err = client.FetchPartitionMetadata(context.Background(), stream, 0)
	require.NoError(t, err)

	// Check that the first readonly timestamp has been updated, and that the
	// latest timestamp has the same value.
	firstReadonlyTimestamp := metadata.ReadonlyTimestamps().FirstTime()
	require.False(t, firstReadonlyTimestamp.IsZero())
	require.Equal(t, firstReadonlyTimestamp, metadata.ReadonlyTimestamps().LatestTime())

	// Set the stream as non-readonly.
	err = client.SetStreamReadonly(context.Background(), stream, lift.Readonly(false))
	require.NoError(t, err)

	metadata, err = client.FetchPartitionMetadata(context.Background(), stream, 0)
	require.NoError(t, err)

	// Check that the first readonly timestamp still has the same value and that
	// the latest timestamp has been updated.
	require.Equal(t, firstReadonlyTimestamp, metadata.ReadonlyTimestamps().FirstTime())
	require.True(t, metadata.ReadonlyTimestamps().LatestTime().After(firstReadonlyTimestamp))
}
