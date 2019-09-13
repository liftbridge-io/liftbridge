# Client Implementation Guidance

This documentation provides guidance on implementing a client library for the
Liftbridge server. If information is missing or inaccurate, feel free to make a
PR fixing it.

## Generating gRPC API

Liftbridge relies on gRPC and Protocol Buffers (protobufs) for its API. These
definitions are stored in the
[liftbridge-grpc](https://github.com/liftbridge-io/liftbridge-grpc) repository.
Refer to the [gRPC documentation](https://grpc.io/docs/) for guidance on
generating the gRPC code from the protobuf definitions for your particular
programming language. The liftbridge-grpc repository also contains generated
code for some languages which you can use in your client library (for example,
the official Go client [relies on](https://github.com/liftbridge-io/go-liftbridge/blob/6fbf530bb220797fd91174d9f858fad3114dbc48/go.mod#L8)
the generated Go code in liftbridge-grpc). If there is no generated code for
your language, feel free to make a PR adding it. Otherwise, you can simply
vendor the generated code alongside your client library.

Generating the gRPC API results in a low-level client that can be used to talk
to Liftbridge. However, it's useful to implement a higher-level client that
provides a more user-friendly abstraction. The remainder of this guide relates
to implementing a high-level client.

## Client Interface

A high-level client currently has only three operations:

- `CreateStream`: creates a new stream attached to a NATS subject (or group of
  related NATS subjects if partitioned)
- `Subscribe`: creates an ephemeral subscription for a given stream that
  messages are received on
- `Publish`: publishes a new message to a NATS subject

Optionally, there is a fourth operation, `Close`, which closes any client
connections to Liftbridge.

Below is the interface definition of the Go Liftbridge client. We'll walk
through each of these operations in more detail below.

```go
// Client is the main API used to communicate with a Liftbridge cluster. Call
// Connect to get a Client instance.
type Client interface {
	// Close the client connection.
	Close() error

	// CreateStream creates a new stream attached to a NATS subject. Subject is
	// the NATS subject the stream is attached to, and name is the stream
	// identifier, unique per subject. It returns ErrStreamExists if a stream
	// with the given subject and name already exists.
	CreateStream(ctx context.Context, subject, name string, opts ...StreamOption) error

	// Subscribe creates an ephemeral subscription for the given stream. It
	// begins receiving messages starting at the configured position and waits
	// for new messages when it reaches the end of the stream. The default
	// start position is the end of the stream. It returns an ErrNoSuchStream
	// if the given stream does not exist. Use a cancelable Context to close a
	// subscription.
	Subscribe(ctx context.Context, stream string, handler Handler, opts ...SubscriptionOption) error

	// Publish publishes a new message to the NATS subject. If the AckPolicy is
	// not NONE and a deadline is provided, this will synchronously block until
	// the first ack is received. If the ack is not received in time, a
	// DeadlineExceeded status code is returned. If an AckPolicy and deadline
	// are configured, this returns the first Ack on success, otherwise it
	// returns nil.
	Publish(ctx context.Context, subject string, value []byte, opts ...MessageOption) (*proto.Ack, error)
}
```

### Close

```go
// Close the client connection.
Close() error
```

`Close` simply releases all resources associated with the client, namely all
gRPC connections to the server. There might be multiple connections since a
client might have connections to multiple servers in a cluster and/or
connection pooling. `Close` takes no arguments and returns/throws an error if
the operation fails.

[Implementation Guidance](#close-implementation)

### CreateStream

```go
// CreateStream creates a new stream attached to a NATS subject. Subject is
// the NATS subject the stream is attached to, and name is the stream
// identifier, unique per subject. It returns ErrStreamExists if a stream
// with the given subject and name already exists.
CreateStream(ctx context.Context, subject, name string, opts ...StreamOption) error
```

`CreateStream` creates a new stream attached to a NATS subject or set of NATS
subjects. Every stream is uniquely identified by a name.

Streams are partitioned for increased parallelism. Each partition maps to a
separate NATS subject. For example, a stream created for the subject `foo` with
three partitions has partitions that map to the NATS subjects `foo`, `foo.1`,
and `foo.2`, respectively. By default, streams consist of just a single
partition.

Each stream partition has a server in the cluster that acts as the leader and
some set of followers which replicate the partition depending on the stream's
replication factor. Liftbridge handles the cases where leaders and followers
fail to maximize availability.

In the Go client example above, `CreateStream` takes four arguments:

- A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for
  passing things like a timeout, cancellation signal, and other values across
  API boundaries. For Liftbridge, this is primarily used for two things:
  request timeouts and cancellation. In other languages, this might be replaced
  by explicit arguments, optional named arguments, or other language-specific
  idioms. Depending on the language, this might be an optional argument.
- A subject string which is the NATS subject to attach the stream to. If the
  stream has more than one partition, this is used as the base subject for each
  partition. This is a required argument.
- A name string which uniquely identifies the stream in the Liftbridge cluster.
  Attempting to create another stream with the same name will result in an
  error. This is a required argument.
- Zero or more stream options. These are used to pass in optional settings for
  the stream. This is a common [Go pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis)
  for implementing extensible APIs. In other languages, this might be replaced
  with a builder pattern or optional named arguments. These `CreateStream`
  options are described below.

The stream options are the equivalent of optional named arguments used to
configure a stream. Supported options are:

- `Group`: string which is the name of a load-balance group for the stream to
  join. When there are multiple streams in the same group, messages on the
  subject will be distributed randomly among them.
- `MaxReplication`: flag which sets the stream replication factor equal to the
  current number of servers in the cluster. This means all partitions for the
  stream will be fully replicated within the cluster.
- `ReplicationFactor`: int which sets the replication factor for the stream.
  The replication factor controls the number of servers a stream's partitions
  should be replicated to. For example, a value of 1 would mean only 1 server
  would have the data, and a value of 3 would mean 3 servers would have it. If
  this is not set, it defaults to 1. A value of -1 will signal to the server to
  set the replication factor equal to the current number of servers in the
  cluster (i.e. MaxReplication).
- `Partitions`: int which sets the number of partitions for the stream. If this
  is not set, it defaults to 1.

`CreateStream` returns/throws an error if the operation fails, specifically
`ErrStreamExists` if a stream with the given name already exists.

[Implementation Guidance](#createstream-implementation)

### Subscribe

```go
// Subscribe creates an ephemeral subscription for the given stream. It
// begins receiving messages starting at the configured position and waits
// for new messages when it reaches the end of the stream. The default
// start position is the end of the stream. It returns an ErrNoSuchStream
// if the given stream does not exist. Use a cancelable Context to close a
// subscription.
Subscribe(ctx context.Context, stream string, handler Handler, opts ...SubscriptionOption) error
```

`Subscribe` is used to consume streams. By default, it begins reading messages
from the end of the stream and blocks for new messages.

In the Go client example above, `Subscribe` takes four arguments:

- A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for
  passing things like a timeout, cancellation signal, and other values across
  API boundaries. For Liftbridge, this is primarily used for two things:
  request timeouts and cancellation. Cancellation is particularly important for
  `Subscribe` because it's how subscriptions are closed. Messages will continue
  to received on a subscription until it is closed. In Go, this is done using a
  cancellable context. In other languages, this might be replaced by an
  explicit argument or other language-specific idiom (e.g.  returning an object
  with a `Cancel` API). Timeouts might be implemented using an explicit
  argument, an optional named argument, or some other idiom.
- A stream name string which is the stream to read messages from.
- A handler callback used to receive messages from the stream, described below.
- Zero or more subscription options. These are used to pass in optional
  settings for the subscription. This is a common [Go pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis)
  for implementing extensible APIs. In other languages, this might be replaced
  with a builder pattern or optional named arguments. These `Subscribe` options
  are described below.

The subscribe handler is a callback function that takes a message and an error.
If the error is not null, the subscription will be terminated and no more
messages will be received. The Go signature of the handler callback is detailed
below. In other languages, this might be a stream mechanism instead of a
callback.

```go
type Handler func(msg *proto.Message, err error)
```

The subscription options are the equivalent of optional named arguments used to
configure a subscription. Supported options are:

- `Partition`: int which specifies the stream partition to consume. If this is
  not set, it defaults to 1.
- `StartAtEarliestReceived`: flag which sets the subscription start position to
  the earliest message received in the stream.
- `StartAtLatestReceived`: flag which sets the subscription start position to
  the last message received in the stream.
- `StartAtOffset`: int which sets the subscription start position to the first
  message with an offset greater than or equal to the given offset.
- `StartAtTime`: timestamp which sets the subscription start position to the
  first message with a timestamp greater than or equal to the given time.
- `StartAtTimeDelta`: time duration which sets the subscription start position
  to the first message with a timestamp greater than or equal to `now - delta`.

`Subscribe` returns/throws an error if the operation fails, specifically
`ErrNoSuchPartition` if the specified stream or partition does not exist.

[Implementation Guidance](#subscribe-implementation)

### Publish

```go
// Publish publishes a new message to the NATS subject. If the AckPolicy is
// not NONE and a deadline is provided, this will synchronously block until
// the first ack is received. If the ack is not received in time, a
// DeadlineExceeded status code is returned. If an AckPolicy and deadline
// are configured, this returns the first Ack on success, otherwise it
// returns nil.
Publish(ctx context.Context, subject string, value []byte, opts ...MessageOption) (*proto.Ack, error)
```

`Publish` sends a message to a NATS subject (and, in turn, any streams that
match the subject). Since Liftbridge streams attach to normal NATS subjects,
it's also possible to [publish messages directly to NATS](https://github.com/liftbridge-io/go-liftbridge#publishing-directly-with-nats)
using a [NATS client](https://nats.io/download/). Liftbridge works fine with
plain, opaque NATS messages, but it also extends NATS with a [protobuf-based
envelope protocol](https://github.com/liftbridge-io/liftbridge-grpc). This
allows publishers to add metadata to messages like a key, headers, and acking
information. Liftbridge client libraries may provide helper methods to make it
easy to create envelopes and deal with acks yourself using a NATS client
directly ([described below](#low-level-publish-helpers)). However, the
`Publish` API is intended to abstract this work away from you.

`Publish` is a synchronous operation, meaning when it returns, the message has
been successfully published. `Publish` can also be configured to block until a
message acknowledgement (ack) is returned from the cluster. This is useful for
ensuring a message has been stored and replicated, guaranteeing at-least-once
delivery. The default ack policy is `LEADER`, meaning the ack is sent once the
partition leader has stored the message.

`Publish` can send messages to particular stream partitions using a
`Partitioner`. In effect, this publishes to a NATS subject derived from the one
passed into `Publish` based on the partition, e.g. `foo`, `foo.1`, `foo.2`,
etc. By default, it publishes to the base subject.

In the Go client example above, `Publish` takes four arguments:

- A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for
  passing things like a timeout, cancellation signal, and other values across
  API boundaries. For Liftbridge, this is primarily used for two things:
  request timeouts and cancellation. In other languages, this might be replaced
  by explicit arguments, optional named arguments, or other language-specific
  idioms. With `Publish`, the timeout is particularly important as it relates
  to acking. If an ack policy is set (see below) and a timeout is provided,
  `Publish` will block until the first ack is received. If the ack is not
  received in time, a timeout error is returned. If the ack policy is `NONE` or
  a timeout is not set, `Publish` returns as soon as the message has been
  published.
- A subject string which is the NATS subject to publish to. This is a required
  argument.
- A message value consisting of opaque bytes. This is a required argument.
- Zero or more message options. These are used to pass in optional settings for
  the message. This is a common [Go pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis)
  for implementing extensible APIs. In other languages, this might be replaced
  with a builder pattern or optional named arguments. These `Publish` options
  are described below.

The publish message options are the equivalent of optional named arguments used
to configure a message. Supported options are:

- `AckInbox`: string which sets the NATS subject Liftbridge should publish the
  message ack to. If this is not set, the server will generate a random inbox.
  This generally does not need to be configured when using the `Publish` API
  since the server will handle acks for you. Instead, it's used if, for some
  reason, you wish to handle the ack yourself.
- `CorrelationID`: string which sets the identifier used to correlate an ack
  with the published message. If it's not set, the ack will not have a
  correlation ID. This generally does not need to be configured when using the
  `Publish` API since the server will handle acks for you. Instead, it's used
  if, for some reason, you wish to handle the ack yourself.
- `AckPolicyAll`: flag which sets the ack policy of the message to `ALL`. This
  means the ack will be sent when the message has been stored by all partition
  replicas.
- `AckPolicyLeader`: flag which sets the ack policy of the message to `LEADER`.
  This means the ack will be sent when the partition leader has stored the
  message. This is the default ack policy if not otherwise set.
- `AckPolicyNone`: flag which sets the ack policy of the message to `NONE`.
  This means no ack will be sent.
- `Header`: string and opaque bytes representing a key-value pair to set as a
  header on the message. This may overwrite previously set headers. Client
  libraries may choose to forgo this option in favor of the bulk `Headers`
  option below. This is a convenience for setting a single header.
- `Headers`: map of strings to opaque bytes representing key-value pairs to set
  as headers on the message. This may overwrite previously set headers.
- `Key`: opaque bytes used for the message key. If Liftbridge has stream
  compaction enabled, the stream will retain only the last message for each
  key (if not set, the message is always retained).
- `PartitionByKey`: flag which maps the message to a stream partition based on
  a hash of the message key. This computes the partition number for the message
  by hashing the key and modding by the number of partitions for the first
  stream found with the subject of the published message. This does not work
  with streams containing wildcards in their subjects, e.g. `foo.*`, since this
  matches on the subject literal of the published message. This also has
  undefined behavior if there are multiple streams for the given subject. This
  is used to derive the actual NATS subject the message is published to, e.g.
  `foo`, `foo.1`, `foo.2`, etc. By default, it's published to the subject
  provided.
- `PartitionByRoundRobin`: flag which maps the message to stream partitions in
  a round-robin fashion. This computes the partition number for the message
  by atomically incrementing a counter for the message subject and modding it
  by the number of partitions for the first stream found with the subject. This
  does not work with streams containing wildcards in their subjects, e.g.
  `foo.*`, since this matches on the subject literal of the published message.
  This also has undefined behavior if there are multiple streams for the given
  subject. This is used to derive the actual NATS subject the message is
  published to, e.g. `foo`, `foo.1`, `foo.2`, etc. By default, it's published
  to the subject provided.
- `PartitionBy`: `Partitioner` (detailed below) which sets the strategy used to
  map the message to a stream partition. This is used to derive the actual NATS
  subject the message is published to, e.g. `foo`, `foo.1`, `foo.2`, etc. By
  default, it's published to the subject provided.
- `ToPartition`: int which sets the partition to publish the message to. If
  this is set, any specified `Partitioner` will not be used. This is used to
  derive the actual NATS subject the message is published to, e.g. `foo`,
  `foo.1`, `foo.2`, etc. By default, it's published to the subject provided.

`Partitioner` is an interface which implements logic for mapping a message to a
stream partition. It passes a `Metadata` object into `Partition`, which is
described later. The Go interface definition is shown below.

```go
// Partitioner is used to map a message to a stream partition.
type Partitioner interface {
	// Partition computes the partition number for a given message.
	Partition(msg *proto.Message, metadata *Metadata) int32
}
```

#### Low-Level Publish Helpers

Because Liftbridge is simply a consumer of NATS, it's also possible to publish
messages directly to NATS rather than using Liftbridge's `Publish` API. This
can be useful for extending the capabilities of NATS in existing systems. This
means we can publish NATS messages as normal, as shown in the example Go code
below, and Liftbridge streams will be able to receive them.

```go
package main

import "github.com/nats-io/nats.go"

func main() {
	// Connect to NATS.
	nc, _ := nats.Connect(nats.DefaultURL)

	// Publish a message.
	nc.Publish("foo.bar", []byte("Hello, world!")) 
	nc.Flush()
}
```

However, these low-level publishes lose out on some of the additional
capabilities of Liftbridge provided by message envelopes, such as message
headers, keys, etc. As a result, client libraries may provide helper methods to
facilitate publishing message envelopes directly to NATS as well as handling
acks. These include `NewMessage`, `UnmarshalAck`, and `UnmarshalMessage`
described below.

##### NewMessage

```go
// NewMessage returns a serialized message for the given payload and options.
func NewMessage(value []byte, options ...MessageOption) []byte
```

`NewMessage` creates a Liftbridge message envelope serialized to bytes ready
for publishing to NATS. This consists of a four bytes ("LIFT"), which is
referred to as the envelope cookie, followed by the serialized message
protobuf. It takes the same arguments as `Publish` (see above) with the
exception of the context and subject.

##### UnmarshalMessage

```go
// UnmarshalMessage deserializes a message from the given byte slice. It
// returns a bool indicating if the given data was actually a Message or not.
func UnmarshalMessage(data []byte) (*proto.Message, bool)
```

`UnmarshalMessage` is a helper method which effectively does the reverse of
`NewMessage`, taking a serialized message and returning a deserialized message
object or indication the data is not actually a message.

##### UnmarshalAck

```go
// UnmarshalAck deserializes an Ack from the given byte slice. It returns an
// error if the given data is not actually an Ack.
func UnmarshalAck(data []byte) (*proto.Ack, error)
```

`UnmarshalAck` is used to deserialize a message ack received on a NATS
subscription. It takes a single argument consisting of the ack bytes as
received from NATS and throws an error/exception if the bytes are not actually
a serialized ack protobuf.

This is useful for handling acks for messages published with a set ack inbox
(i.e. the NATS subject Liftbridge will publish message acks to). For example:

```go
// Connect to NATS.
nc, _ := nats.Connect(nats.DefaultURL)

// Subscribe to ack inbox.
nc.Subscribe("my.ack.inbox", func(msg *nats.Msg) {
    ack, _ := lift.UnmarshalAck(msg.Data)
    // CorrelationId can be used to correlate an ack with a particular message.
    if ack.CorrelationId == "123" {
        println("message acked!")
    }
})

// Publish a message envelope.
nc.Publish("foo.bar", lift.NewMessage(
    []byte("hello"),
    lift.AckInbox("my.ack.inbox"),
    lift.CorrelationID("123"),
))
```
