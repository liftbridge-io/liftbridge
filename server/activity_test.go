package server

import (
	"context"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	liftApi "github.com/liftbridge-io/liftbridge-api/go"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
)

// Ensure activity stream creation event occurs.
func TestActivityStreamCreateStream(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.ActivityStream.Enabled = true
	s1Config.ActivityStream.PublishTimeout = time.Second
	s1Config.ActivityStream.PublishAckPolicy = liftApi.AckPolicy_LEADER
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// The first message read back should be the creation of the activity
	// stream partition.
	msgs := make(chan *lift.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the message.
	select {
	case msg := <-msgs:
		var se liftApi.ActivityStreamEvent
		err = se.Unmarshal(msg.Value())
		require.NoError(t, err)
		require.Equal(t, liftApi.ActivityStreamOp_CREATE_STREAM, se.GetOp())
		require.Equal(t, activityStream, se.CreateStreamOp.GetStream())
		require.Equal(t, 1, len(se.CreateStreamOp.GetPartitions()))
		require.Equal(t, int32(0), se.CreateStreamOp.GetPartitions()[0])
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure activity stream deletion event occurs.
func TestActivityStreamDeleteStream(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.ActivityStream.Enabled = true
	s1Config.ActivityStream.PublishTimeout = time.Second
	s1Config.ActivityStream.PublishAckPolicy = liftApi.AckPolicy_LEADER
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	stream := "foo-stream"
	require.NoError(t, client.CreateStream(context.Background(), "foo", stream))

	// Delete stream.
	require.NoError(t, client.DeleteStream(context.Background(), stream))

	// The first message read back should be the stream deletion.
	msgs := make(chan *lift.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtLatestReceived())
	require.NoError(t, err)

	// Wait to get the message.
	select {
	case msg := <-msgs:
		var se liftApi.ActivityStreamEvent
		err = se.Unmarshal(msg.Value())
		require.NoError(t, err)
		require.Equal(t, liftApi.ActivityStreamOp_DELETE_STREAM, se.GetOp())
		require.Equal(t, stream, se.DeleteStreamOp.GetStream())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure activity stream pause event occurs.
func TestActivityStreamPauseStream(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.ActivityStream.Enabled = true
	s1Config.ActivityStream.PublishTimeout = time.Second
	s1Config.ActivityStream.PublishAckPolicy = liftApi.AckPolicy_LEADER
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	stream := "foo-stream"
	require.NoError(t, client.CreateStream(context.Background(), "foo", stream))

	// Pause stream.
	require.NoError(t, client.PauseStream(context.Background(), stream))

	// The first message read back should be the stream pause.
	msgs := make(chan *lift.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtLatestReceived())
	require.NoError(t, err)

	// Wait to get the message.
	select {
	case msg := <-msgs:
		var se liftApi.ActivityStreamEvent
		err = se.Unmarshal(msg.Value())
		require.NoError(t, err)
		require.Equal(t, liftApi.ActivityStreamOp_PAUSE_STREAM, se.GetOp())
		require.Equal(t, stream, se.PauseStreamOp.GetStream())
		require.False(t, se.PauseStreamOp.GetResumeAll())
		require.Equal(t, []int32{0}, se.PauseStreamOp.GetPartitions())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure activity stream resume event occurs.
func TestActivityStreamResumeStream(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.ActivityStream.Enabled = true
	s1Config.ActivityStream.PublishTimeout = time.Second
	s1Config.ActivityStream.PublishAckPolicy = liftApi.AckPolicy_LEADER
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	stream := "foo-stream"
	require.NoError(t, client.CreateStream(context.Background(), "foo", stream))

	// Pause stream.
	require.NoError(t, client.PauseStream(context.Background(), stream))

	// Resume stream by publishing to it.
	_, err = client.Publish(context.Background(), stream, []byte("foo"))
	require.NoError(t, err)

	// The first message read back should be the stream resume.
	msgs := make(chan *lift.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtLatestReceived())
	require.NoError(t, err)

	// Wait to get the message.
	select {
	case msg := <-msgs:
		var se liftApi.ActivityStreamEvent
		err = se.Unmarshal(msg.Value())
		require.NoError(t, err)
		require.Equal(t, liftApi.ActivityStreamOp_RESUME_STREAM, se.GetOp())
		require.Equal(t, stream, se.ResumeStreamOp.GetStream())
		require.Equal(t, []int32{0}, se.ResumeStreamOp.GetPartitions())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure activity stream set readonly (on) event occurs.
func TestActivityStreamSetStreamReadonly(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.ActivityStream.Enabled = true
	s1Config.ActivityStream.PublishTimeout = time.Second
	s1Config.ActivityStream.PublishAckPolicy = liftApi.AckPolicy_LEADER
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	stream := "foo-stream"
	require.NoError(t, client.CreateStream(context.Background(), "foo", stream))

	// Set stream readonly.
	require.NoError(t, client.SetStreamReadonly(context.Background(), stream))

	// The first message read back should be the stream readonly.
	msgs := make(chan *lift.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtLatestReceived())
	require.NoError(t, err)

	// Wait to get the message.
	select {
	case msg := <-msgs:
		var se liftApi.ActivityStreamEvent
		err = se.Unmarshal(msg.Value())
		require.NoError(t, err)
		require.Equal(t, liftApi.ActivityStreamOp_SET_STREAM_READONLY, se.GetOp())
		require.Equal(t, stream, se.SetStreamReadonlyOp.GetStream())
		require.Equal(t, []int32{0}, se.SetStreamReadonlyOp.GetPartitions())
		require.Equal(t, true, se.SetStreamReadonlyOp.GetReadonly())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure activity stream set readonly (off) event occurs.
func TestActivityStreamSetStreamReadwrite(t *testing.T) {
	defer cleanupStorage(t)

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1Config := getTestConfig("a", true, 5050)
	s1Config.ActivityStream.Enabled = true
	s1Config.ActivityStream.PublishTimeout = time.Second
	s1Config.ActivityStream.PublishAckPolicy = liftApi.AckPolicy_LEADER
	s1 := runServerWithConfig(t, s1Config)
	defer s1.Stop()

	// Wait for server to elect itself leader.
	getMetadataLeader(t, 10*time.Second, s1)

	client, err := lift.Connect([]string{"localhost:5050"})
	require.NoError(t, err)
	defer client.Close()

	// Create stream.
	stream := "foo-stream"
	require.NoError(t, client.CreateStream(context.Background(), "foo", stream))

	// Set stream readonly.
	require.NoError(t, client.SetStreamReadonly(context.Background(), stream))

	// Set stream readwrite.
	require.NoError(t, client.SetStreamReadonly(context.Background(), stream, lift.Readonly(false)))

	// The first message read back should be the stream readwrite.
	msgs := make(chan *lift.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtLatestReceived())
	require.NoError(t, err)

	// Wait to get the message.
	select {
	case msg := <-msgs:
		var se liftApi.ActivityStreamEvent
		err = se.Unmarshal(msg.Value())
		require.NoError(t, err)
		require.Equal(t, liftApi.ActivityStreamOp_SET_STREAM_READONLY, se.GetOp())
		require.Equal(t, stream, se.SetStreamReadonlyOp.GetStream())
		require.Equal(t, []int32{0}, se.SetStreamReadonlyOp.GetPartitions())
		require.Equal(t, false, se.SetStreamReadonlyOp.GetReadonly())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// Ensure computeActivityPublishBackoff doubles the backoff time and caps it at
// the max backoff.
func TestComputeActivityPublishBackoff(t *testing.T) {
	var backoff time.Duration
	backoff = computeActivityPublishBackoff(backoff)
	require.Equal(t, time.Second, backoff)
	backoff = computeActivityPublishBackoff(backoff)
	require.Equal(t, 2*time.Second, backoff)
	backoff = computeActivityPublishBackoff(backoff)
	require.Equal(t, 4*time.Second, backoff)
	backoff = computeActivityPublishBackoff(backoff)
	require.Equal(t, 8*time.Second, backoff)
	backoff = computeActivityPublishBackoff(backoff)
	require.Equal(t, 10*time.Second, backoff)
	backoff = computeActivityPublishBackoff(backoff)
	require.Equal(t, 10*time.Second, backoff)
}
