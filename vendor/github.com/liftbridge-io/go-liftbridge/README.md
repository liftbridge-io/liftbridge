# go-liftbridge [![CircleCI](https://circleci.com/gh/liftbridge-io/go-liftbridge.svg?style=svg)](https://circleci.com/gh/liftbridge-io/go-liftbridge) [![GoDoc](https://godoc.org/github.com/liftbridge-io/go-liftbridge?status.svg)](https://godoc.org/github.com/liftbridge-io/go-liftbridge)

Go client for [Liftbridge](https://github.com/liftbridge-io/liftbridge), a
system that provides lightweight, fault-tolerant message streams for
[NATS](https://nats.io).

Liftbridge provides the following high-level features:

- Log-based API for NATS
- Replicated for fault-tolerance
- Horizontally scalable
- Wildcard subscription support
- At-least-once delivery support and message replay
- Message key-value support
- Log compaction by key (WIP)

## Installation

```
$ go get github.com/liftbridge-io/go-liftbridge
```

## Basic Usage

```go
package main

import (
	"fmt"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
	"golang.org/x/net/context"
)

func main() {
	// Create Liftbridge client.
	addrs := []string{"localhost:9292", "localhost:9293", "localhost:9294"}
	client, err := lift.Connect(addrs)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create a stream attached to the NATS subject "foo".
	stream := lift.StreamInfo{
		Subject: "foo",
		Name:    "foo-stream",
	}
	if err := client.CreateStream(context.Background(), stream); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}

	// Subscribe to the stream starting from the beginning.
	ctx := context.Background()
	if err := client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Offset, string(msg.Value))
	}, lift.StartAtEarliestReceived()); err != nil {
		panic(err)
	}

	<-ctx.Done()
}
```

### Create Stream

[Streams](https://github.com/liftbridge-io/liftbridge/blob/master/documentation/concepts.md#stream)
are a durable message log attached to a NATS subject. They record messages
published to the subject for consumption.

Streams have a few key properties: a subject, which is the corresponding NATS
subject, a name, which is a human-readable identifier for the stream, and a
replication factor, which is the number of nodes the stream should be
replicated to for redundancy.  Optionally, there is a group which is the name
of a load-balance group for the stream to join. When there are multiple streams
in the same group, messages will be balanced among them.

```go
// Create a stream attached to the NATS subject "foo.*".
stream := lift.StreamInfo{
    Subject:           "foo.*",
    Name:              "my-stream",
    ReplicationFactor: lift.MaxReplicationFactor, // Replicate to all the brokers in the cluster
}

// Create the stream. ErrStreamExists is returned if a stream with the given
// name already exists for the subject.
client.CreateStream(context.Background(), stream)
```

### Subscription Start/Replay Options

[Subscriptions](https://github.com/liftbridge-io/liftbridge/blob/master/documentation/concepts.md#subscription)
are how Liftbridge streams are consumed. Clients can choose where to start
consuming messages from in a stream. This is controlled using options passed to
Subscribe.

```go
// Subscribe starting with new messages only.
client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
    fmt.Println(msg.Offset, string(msg.Value))
})

// Subscribe starting with the most recently published value.
client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
    fmt.Println(msg.Offset, string(msg.Value))
}, lift.StartAtLatestReceived())

// Subscribe starting with the oldest published value.
client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
    fmt.Println(msg.Offset, string(msg.Value))
}, lift.StartAtEarliestReceived())

// Subscribe starting at a specific offset.
client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
    fmt.Println(msg.Offset, string(msg.Value))
}, lift.StartAtOffset(42))

// Subscribe starting at a specific time.
client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
    fmt.Println(msg.Offset, string(msg.Value))
}, lift.StartAtTime(time.Now()))

// Subscribe starting at a specific amount of time in the past.
client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *proto.Message, err error) {
    fmt.Println(msg.Offset, string(msg.Value))
}, lift.StartAtTimeDelta(time.Minute))
```

### Publishing

Since Liftbridge is simply an extension of
[NATS](https://github.com/nats-io/gnatsd), a [NATS
client](https://github.com/nats-io/go-nats) is used to publish messages. This
means existing NATS publishers do not need any changes for messages to be
consumed in Liftbridge.

```go
package main

import "github.com/nats-io/go-nats"

func main() {
    // Connect to NATS.
    nc, _ := nats.Connect(nats.DefaultURL)

    // Publish a message.
    nc.Publish("foo.bar", []byte("Hello, world!")) 
    nc.Flush()
}
```

Liftbridge allows publishers to add metadata to messages, including a key, ack
inbox, correlation ID, and ack policy. The message key can be used for stream
compaction in Liftbridge. Acks are used to guarantee Liftbridge has recorded a
message to ensure at-least-once delivery. The ack inbox determines a NATS
subject to publish an acknowledgement to once Liftbridge has committed the
message. The correlation id is used to correlate an ack back to the original
message. The ack policy determines when Liftbridge acknowledges the message:
when the stream leader has stored the message, when all replicas have stored
it, or no ack at all.

This additional metadata is sent using a message envelope which is a
[protobuf](https://github.com/liftbridge-io/liftbridge-grpc). This client
library provides APIs to make it easy to create envelopes and deal with acks.

```go
var (
    ackInbox = "foo.acks"
    cid      = "some-random-id"
)

// Create a message envelope to publish.
msg := lift.NewMessage([]byte("Hello, world!"), lift.MessageOptions{
    Key:           []byte("foo"),       // Key to set on the message
    AckInbox:      ackInbox,            // Send ack to this NATS subject
    AckPolicy:     proto.AckPolicy_ALL, // Send ack once message is fully replicated
    CorrelationID: cid,                 // Set the ID which will be sent on the ack
})

// Setup a NATS subscription for acks.
sub, _ := nc.SubscribeSync(ackInbox)

// Publish the message.
nc.Publish("foo.bar", msg)

// Wait for ack from Liftbridge.
resp, _ := sub.NextMsg(5*time.Second)
ack, _ := lift.UnmarshalAck(resp.Data)
if ack.CorrelationId == cid {
    fmt.Println("message acked!")
}
```
