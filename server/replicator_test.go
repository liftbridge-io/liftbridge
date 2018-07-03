package server

import (
	"strconv"
	"testing"
	"time"

	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/require"
	"github.com/tylertreat/go-liftbridge"
	"github.com/tylertreat/go-liftbridge/liftbridge-grpc"
	"golang.org/x/net/context"
)

func waitForHW(t *testing.T, timeout time.Duration, subject, name string, hw int64, servers ...*Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			stream := s.metadata.GetStream(subject, name)
			if stream == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
			if stream.log.HighWatermark() < hw {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
		}
		return
	}
	stackFatalf(t, "Cluster did not reach HW %d for [subject=%s, name=%s]", hw, subject, name)
}

func waitForStream(t *testing.T, timeout time.Duration, subject, name string, servers ...*Server) {
	deadline := time.Now().Add(timeout)
LOOP:
	for time.Now().Before(deadline) {
		for _, s := range servers {
			stream := s.metadata.GetStream(subject, name)
			if stream == nil {
				time.Sleep(15 * time.Millisecond)
				continue LOOP
			}
		}
		return
	}
	stackFatalf(t, "Cluster did not create stream [subject=%s, name=%s]", subject, name)
}

// Ensure messages are replicated and the stream leader fails over when the
// leader dies.
func TestStreamLeaderFailover(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.Clustering.ReplicaMaxLeaderTimeout = 2 * time.Second
	s1Config.Clustering.ReplicaFetchTimeout = time.Second
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Configure second server.
	s2Config := getTestConfig("b", false, 5051)
	s2Config.Clustering.ReplicaMaxLeaderTimeout = 2 * time.Second
	s2Config.Clustering.ReplicaFetchTimeout = time.Second
	s2 := runServerWithConfig(t, s2Config)
	defer s2.Stop()

	// Configure second server.
	s3Config := getTestConfig("c", false, 5052)
	s3Config.Clustering.ReplicaMaxLeaderTimeout = 2 * time.Second
	s3Config.Clustering.ReplicaFetchTimeout = time.Second
	s3 := runServerWithConfig(t, s3Config)
	defer s3.Stop()

	servers := []*Server{s1, s2, s3}
	getMetadataLeader(t, 10*time.Second, servers...)

	client, err := liftbridge.Connect("localhost:5050", "localhost:5051", "localhost:5052")
	require.NoError(t, err)
	defer client.Close()

	stream := liftbridge.StreamInfo{
		Name:              "foo",
		Subject:           "foo",
		ReplicationFactor: 3,
	}
	err = client.CreateStream(context.Background(), stream)
	require.NoError(t, err)

	num := 100
	expected := make([]*message, num)
	for i := 0; i < num; i++ {
		expected[i] = &message{
			Key:    []byte("bar"),
			Value:  []byte(strconv.Itoa(i)),
			Offset: int64(i),
		}
	}

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
		if acksRecv == num {
			close(gotAcks)
		}
	})
	require.NoError(t, err)
	nc.Flush()

	// Publish messages.
	for i := 0; i < num; i++ {
		err = nc.Publish(stream.Subject,
			liftbridge.NewEnvelope(expected[i].Key, expected[i].Value, acks))
		require.NoError(t, err)
	}

	// Make sure we got all the acks.
	select {
	case <-gotAcks:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected acks")
	}

	// Make sure we can play back the log.
	i := 0
	ch := make(chan struct{})
	err = client.Subscribe(context.Background(), stream.Subject, stream.Name,
		0, func(msg *proto.Message, err error) {
			if i == num && err != nil {
				return
			}
			require.NoError(t, err)
			expect := expected[i]
			assertMsg(t, expect, msg)
			i++
			if i == num {
				close(ch)
			}
		})
	require.NoError(t, err)

	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}

	// Wait for HW to update on followers.
	waitForHW(t, 5*time.Second, stream.Subject, stream.Name, int64(num-1), servers...)

	// Kill the stream leader.
	leader := getStreamLeader(t, 10*time.Second, stream.Subject, stream.Name, servers...)
	leader.Stop()
	followers := []*Server{}
	for _, s := range servers {
		if s == leader {
			continue
		}
		followers = append(followers, s)
	}

	// Wait for new leader to be elected.
	getStreamLeader(t, 10*time.Second, stream.Subject, stream.Name, followers...)

	// Make sure the new leader's log is consistent.
	i = 0
	ch = make(chan struct{})
	err = client.Subscribe(context.Background(), stream.Subject, stream.Name,
		0, func(msg *proto.Message, err error) {
			if i == num && err != nil {
				return
			}
			require.NoError(t, err)
			expect := expected[i]
			assertMsg(t, expect, msg)
			i++
			if i == num {
				close(ch)
			}
		})
	require.NoError(t, err)

	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("Did not receive all expected messages")
	}
}
