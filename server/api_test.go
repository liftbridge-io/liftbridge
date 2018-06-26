package server

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/require"
	"github.com/tylertreat/go-liftbridge"
	"github.com/tylertreat/go-liftbridge/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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

	client, err := liftbridge.Connect("localhost:5050")
	require.NoError(t, err)

	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name, 1)
	require.NoError(t, err)

	// Creating the same stream returns ErrStreamExists.
	err = client.CreateStream(context.Background(), subject, name, 1)
	require.Equal(t, liftbridge.ErrStreamExists, err)
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
	client, err := liftbridge.Connect("localhost:5050")
	require.NoError(t, err)

	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name, 1)
	require.NoError(t, err)

	// Creating the same stream returns ErrStreamExists.
	err = client.CreateStream(context.Background(), subject, name, 1)
	require.Equal(t, liftbridge.ErrStreamExists, err)
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

	client, err := liftbridge.Connect("localhost:5050")
	require.NoError(t, err)

	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name, 2)
	require.Error(t, err)
}

// Ensure subscribing to a non-existant stream returns an error.
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

	stream, err := apiClient.Subscribe(context.Background(), &proto.SubscribeRequest{
		Subject: "foo",
		Name:    "foo",
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "No such stream")
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
	client, err := liftbridge.Connect("localhost:5050")
	require.NoError(t, err)

	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name, 2)
	require.NoError(t, err)

	require.NoError(t, client.Close())

	// Wait for both nodes to create stream.
	waitForStream(t, 5*time.Second, subject, name, s1, s2)

	// Connect to the server that is the stream follower.
	leader := getStreamLeader(t, 10*time.Second, subject, name, s1, s2)
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
	stream, err := apiClient.Subscribe(context.Background(), &proto.SubscribeRequest{
		Subject: subject,
		Name:    name,
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "Server not stream leader")
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

	client, err := liftbridge.Connect("localhost:5050")
	require.NoError(t, err)

	name := "foo"
	subject := "foo"
	err = client.CreateStream(context.Background(), subject, name, 1)
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
	err = client.Subscribe(context.Background(), subject, name, 0, func(msg *proto.Message, err error) {
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

	nc, err := nats.GetDefaultOptions().Connect()
	require.NoError(t, err)
	defer nc.Close()

	// Subscribe to acks.
	acks := "acks"
	acksRecv := 0
	gotAcks := make(chan struct{})
	_, err = nc.Subscribe(acks, func(m *nats.Msg) {
		_, err := liftbridge.UnmarshalAck(m.Data)
		require.NoError(t, err)
		acksRecv++
		if acksRecv == num+5 {
			close(gotAcks)
		}
	})
	require.NoError(t, err)

	// Publish messages.
	for i := 0; i < num; i++ {
		err = nc.Publish(subject, liftbridge.NewEnvelope(expected[i].Key, expected[i].Value, acks))
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
		err = nc.Publish(subject, liftbridge.NewEnvelope(expected[i+num].Key, expected[i+num].Value, acks))
		require.NoError(t, err)
	}

	// Wait to receive remaining messages.
	select {
	case <-ch2:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}

	// Make sure we got all the acks.
	select {
	case <-gotAcks:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected acks")
	}

	// Make sure we can play back the log.
	client2, err := liftbridge.Connect("localhost:5050")
	require.NoError(t, err)
	defer client2.Close()
	i = num
	ch1 = make(chan struct{})
	err = client2.Subscribe(context.Background(), subject, name, int64(num), func(msg *proto.Message, err error) {
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
	})
	require.NoError(t, err)

	select {
	case <-ch1:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}
}
