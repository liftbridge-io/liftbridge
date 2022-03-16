package server

import (
	"context"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
	liftApi "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/stretchr/testify/require"
)

// Ensure activity stream creation event occurs.
func TestActivityStreamCreateStream(t *testing.T) {
	defer cleanupStorage(t)

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

	// The last message read back should be the stream deletion.
	var (
		msgs  = make(chan *lift.Message, 1)
		count = 0
	)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		count++
		msgs <- msg
		if count == 3 {
			cancel()
		}
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the message (ignore first two because they are for the
	// creation of the activity stream and foo stream).
	for i := 0; i < 3; i++ {
		select {
		case msg := <-msgs:
			if i == 2 {
				var se liftApi.ActivityStreamEvent
				err = se.Unmarshal(msg.Value())
				require.NoError(t, err)
				require.Equal(t, liftApi.ActivityStreamOp_DELETE_STREAM, se.GetOp())
				require.Equal(t, stream, se.DeleteStreamOp.GetStream())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Did not receive expected message")
		}
	}
}

// Ensure activity stream pause event occurs.
func TestActivityStreamPauseStream(t *testing.T) {
	defer cleanupStorage(t)

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

	// The last message read back should be the stream pause.
	var (
		msgs  = make(chan *lift.Message, 1)
		count = 0
	)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		count++
		msgs <- msg
		if count == 3 {
			cancel()
		}
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the message (ignore first two because they are for the
	// creation of the activity stream and foo stream).
	for i := 0; i < 3; i++ {
		select {
		case msg := <-msgs:
			if i == 2 {
				var se liftApi.ActivityStreamEvent
				err = se.Unmarshal(msg.Value())
				require.NoError(t, err)
				require.Equal(t, liftApi.ActivityStreamOp_PAUSE_STREAM, se.GetOp())
				require.Equal(t, stream, se.PauseStreamOp.GetStream())
				require.False(t, se.PauseStreamOp.GetResumeAll())
				require.Equal(t, []int32{0}, se.PauseStreamOp.GetPartitions())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Did not receive expected message")
		}
	}
}

// Ensure activity stream resume event occurs.
func TestActivityStreamResumeStream(t *testing.T) {
	defer cleanupStorage(t)

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

	// The last message read back should be the stream resume.
	var (
		msgs  = make(chan *lift.Message, 1)
		count = 0
	)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		count++
		msgs <- msg
		if count == 4 {
			cancel()
		}
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the message (ignore first three because they are for the
	// creation of the activity stream, creation of foo stream, and pausing of
	// foo stream).
	for i := 0; i < 4; i++ {
		select {
		case msg := <-msgs:
			if i == 3 {
				var se liftApi.ActivityStreamEvent
				err = se.Unmarshal(msg.Value())
				require.NoError(t, err)
				require.Equal(t, liftApi.ActivityStreamOp_RESUME_STREAM, se.GetOp())
				require.Equal(t, stream, se.ResumeStreamOp.GetStream())
				require.Equal(t, []int32{0}, se.ResumeStreamOp.GetPartitions())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Did not receive expected message")
		}
	}
}

// Ensure activity stream set readonly (on) event occurs.
func TestActivityStreamSetStreamReadonly(t *testing.T) {
	defer cleanupStorage(t)

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

	// The last message read back should be the stream readonly.
	var (
		msgs  = make(chan *lift.Message, 1)
		count = 0
	)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		count++
		msgs <- msg
		if count == 3 {
			cancel()
		}
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the message (ignore first two because they are for the
	// creation of the activity stream and foo stream).
	for i := 0; i < 3; i++ {
		select {
		case msg := <-msgs:
			if i == 2 {
				var se liftApi.ActivityStreamEvent
				err = se.Unmarshal(msg.Value())
				require.NoError(t, err)
				require.Equal(t, liftApi.ActivityStreamOp_SET_STREAM_READONLY, se.GetOp())
				require.Equal(t, stream, se.SetStreamReadonlyOp.GetStream())
				require.Equal(t, []int32{0}, se.SetStreamReadonlyOp.GetPartitions())
				require.Equal(t, true, se.SetStreamReadonlyOp.GetReadonly())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Did not receive expected message")
		}
	}
}

// Ensure activity stream set readonly (off) event occurs.
func TestActivityStreamSetStreamReadwrite(t *testing.T) {
	defer cleanupStorage(t)

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

	// The last message read back should be the stream readwrite.
	var (
		msgs  = make(chan *lift.Message, 1)
		count = 0
	)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		count++
		msgs <- msg
		if count == 4 {
			cancel()
		}
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the message (ignore first three because they are for the
	// creation of the activity stream, creation of foo stream, and setting foo
	// stream to readonly).
	for i := 0; i < 4; i++ {
		select {
		case msg := <-msgs:
			if i == 3 {
				var se liftApi.ActivityStreamEvent
				err = se.Unmarshal(msg.Value())
				require.NoError(t, err)
				require.Equal(t, liftApi.ActivityStreamOp_SET_STREAM_READONLY, se.GetOp())
				require.Equal(t, stream, se.SetStreamReadonlyOp.GetStream())
				require.Equal(t, []int32{0}, se.SetStreamReadonlyOp.GetPartitions())
				require.Equal(t, false, se.SetStreamReadonlyOp.GetReadonly())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Did not receive expected message")
		}
	}
}

// Ensure activity stream join and leave consumer group events occur.
func TestActivityStreamJoinLeaveConsumerGroup(t *testing.T) {
	defer cleanupStorage(t)

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

	var (
		group     = "my-group"
		consumer1 = "my-consumer-1"
		consumer2 = "my-consumer-2"
	)

	// Join consumer group (this results in the group being created).
	cons1, err := client.CreateConsumer(group, lift.ConsumerID(consumer1))
	require.NoError(t, err)
	err = cons1.Subscribe(context.Background(), []string{stream}, func(msg *lift.Message, err error) {})
	require.NoError(t, err)

	// The last two messages read back should be the consumer group join and
	// leave.
	var (
		msgs  = make(chan *lift.Message, 1)
		count = 0
	)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.Subscribe(ctx, activityStream, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		count++
		msgs <- msg
		if count == 5 {
			cancel()
		}
	}, lift.StartAtEarliestReceived())
	require.NoError(t, err)

	// Wait to get the message (ignore first two because they are for the
	// creation of the activity stream and foo stream).
	for i := 0; i < 3; i++ {
		select {
		case msg := <-msgs:
			if i == 2 {
				var se liftApi.ActivityStreamEvent
				err = se.Unmarshal(msg.Value())
				require.NoError(t, err)
				require.Equal(t, liftApi.ActivityStreamOp_JOIN_CONSUMER_GROUP, se.GetOp())
				require.Equal(t, group, se.JoinConsumerGroupOp.GetGroupId())
				require.Equal(t, consumer1, se.JoinConsumerGroupOp.GetConsumerId())
				require.Equal(t, []string{stream}, se.JoinConsumerGroupOp.GetStreams())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Did not receive expected message")
		}
	}

	// Join another consumer.
	cons2, err := client.CreateConsumer(group, lift.ConsumerID(consumer2))
	require.NoError(t, err)
	err = cons2.Subscribe(context.Background(), []string{stream}, func(msg *lift.Message, err error) {})
	require.NoError(t, err)

	// Wait to get the message.
	select {
	case msg := <-msgs:
		var se liftApi.ActivityStreamEvent
		err = se.Unmarshal(msg.Value())
		require.NoError(t, err)
		require.Equal(t, liftApi.ActivityStreamOp_JOIN_CONSUMER_GROUP, se.GetOp())
		require.Equal(t, group, se.JoinConsumerGroupOp.GetGroupId())
		require.Equal(t, consumer2, se.JoinConsumerGroupOp.GetConsumerId())
		require.Equal(t, []string{stream}, se.JoinConsumerGroupOp.GetStreams())
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}

	// Leave consumer group.
	require.NoError(t, cons1.Close())

	// Wait to get the message.
	select {
	case msg := <-msgs:
		var se liftApi.ActivityStreamEvent
		err = se.Unmarshal(msg.Value())
		require.NoError(t, err)
		require.Equal(t, liftApi.ActivityStreamOp_LEAVE_CONSUMER_GROUP, se.GetOp())
		require.Equal(t, group, se.LeaveConsumerGroupOp.GetGroupId())
		require.Equal(t, consumer1, se.LeaveConsumerGroupOp.GetConsumerId())
		require.False(t, se.LeaveConsumerGroupOp.GetExpired())
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
