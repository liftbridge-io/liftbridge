---
id: client-implementation
title: Client Implementation Guidance
---

This documentation provides guidance on implementing a [client
library](clients.md) for the Liftbridge server. If information is missing or
inaccurate, feel free to make a PR fixing it.

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

A high-level client has several operations:

- [`CreateStream`](#createstream): creates a new stream attached to a NATS subject (or group of
  related NATS subjects if partitioned)
- [`Subscribe`](#subscribe): creates an ephemeral subscription for a given stream that
  messages are received on
- [`Publish`](#publish): publishes a new message to a NATS subject
- [`FetchMetadata`](#fetchmetadata): retrieves metadata from the cluster
- [`Close`](#close): closes any client connections to Liftbridge

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

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. | language-dependent |
| subject | string | A subject string which is the NATS subject to attach the stream to. If the stream has more than one partition, this is used as the base subject for each partition. | yes |
| name | string | A name string which uniquely identifies the stream in the Liftbridge cluster. Attempting to create another stream with the same name will result in an error. | yes |
| options | stream options | Zero or more stream options. These are used to pass in optional settings for the stream. This is a common [Go pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) for implementing extensible APIs. In other languages, this might be replaced with a builder pattern or optional named arguments. These `CreateStream` options are described below. | language-dependent |

The stream options are the equivalent of optional named arguments used to
configure a stream. Supported options are:

| Option | Type | Description | Default |
|:----|:----|:----|:----|
| Group | string | The name of a load-balance group for the stream to join. When there are multiple streams in the same group, messages on the subject will be distributed randomly among them. | |
| MaxReplication | bool | Sets the stream replication factor equal to the current number of servers in the cluster. This means all partitions for the stream will be fully replicated within the cluster. | false |
| ReplicationFactor | int | Sets the replication factor for the stream. The replication factor controls the number of servers a stream's partitions should be replicated to. For example, a value of 1 would mean only 1 server would have the data, and a value of 3 would mean 3 servers would have it. A value of -1 will signal to the server to set the replication factor equal to the current number of servers in the cluster (i.e. MaxReplication). | 1 |
| Partitions | int | Sets the number of partitions for the stream. | 1 |

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

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. Cancellation is particularly important for `Subscribe` because it's how subscriptions are closed. Messages will continue to received on a subscription until it is closed. In Go, this is done using a cancellable context. In other languages, this might be replaced by an explicit argument or other language-specific idiom (e.g.  returning an object with a `Cancel` API). Timeouts might be implemented using an explicit argument, an optional named argument, or some other idiom. | language-dependent |
| stream | string | A stream name string which is the stream to read messages from. | yes |
| handler | callback | A handler callback used to receive messages from the stream, described below. | yes |
| options | subscription options | Zero or more subscription options. These are used to pass in optional settings for the subscription. This is a common [Go pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) for implementing extensible APIs. In other languages, this might be replaced with a builder pattern or optional named arguments. These `Subscribe` options are described below. | language-dependent |

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

| Option | Type | Description | Default |
|:----|:----|:----|:----|
| Partition | int | Specifies the stream partition to consume. | 0 |
| StartAtEarliestReceived | bool | Sets the subscription start position to the earliest message received in the stream. | false |
| StartAtLatestReceived | bool | Sets the subscription start position to the last message received in the stream. | false |
| StartAtOffset | int | Sets the subscription start position to the first message with an offset greater than or equal to the given offset. | |
| StartAtTime | timestamp | Sets the subscription start position to the first message with a timestamp greater than or equal to the given time. | |
| StartAtTimeDelta | time duration | Sets the subscription start position to the first message with a timestamp greater than or equal to `now - delta`. | |

Currently, `Subscribe` can only subscribe to a single partition. In the future,
there will be functionality for consuming all partitions.

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

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. With `Publish`, the timeout is particularly important as it relates to acking. If an ack policy is set (see below) and a timeout is provided, `Publish` will block until the first ack is received. If the ack is not received in time, a timeout error is returned. If the ack policy is `NONE` or a timeout is not set, `Publish` returns as soon as the message has been published. | language-dependent |
| subject | string | The NATS subject to publish to. | yes |
| value | bytes | The message value to publish consisting of opaque bytes. | yes |
| options | message options | Zero or more message options. These are used to pass in optional settings for the message. This is a common [Go pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) for implementing extensible APIs. In other languages, this might be replaced with a builder pattern or optional named arguments. These `Publish` options are described below. | language-dependent |

The publish message options are the equivalent of optional named arguments used
to configure a message. Supported options are:

| Option | Type | Description | Default |
|:----|:----|:----|:----|
| AckInbox | string | Sets the NATS subject Liftbridge should publish the message ack to. If this is not set, the server will generate a random inbox. This generally does not need to be configured when using the `Publish` API since the server will handle acks for you. Instead, it's used if, for some reason, you wish to handle the ack yourself. | |
| CorrelationID | string | Sets the identifier used to correlate an ack with the published message. If it's not set, the ack will not have a correlation ID. This generally does not need to be configured when using the `Publish` API since the server will handle acks for you. Instead, it's used if, for some reason, you wish to handle the ack yourself. | |
| AckPolicyAll | bool | Sets the ack policy of the message to `ALL`. This means the ack will be sent when the message has been stored by all partition replicas. | false |
| AckPolicyLeader | bool | Sets the ack policy of the message to `LEADER`. This means the ack will be sent when the partition leader has stored the message. This is the default ack policy if not otherwise set. | true |
| AckPolicyNone | bool | Sets the ack policy of the message to `NONE`. This means no ack will be sent. | false |
| Header | string and bytes | String and opaque bytes representing a key-value pair to set as a header on the message. This may overwrite previously set headers. Client libraries may choose to forgo this option in favor of the bulk `Headers` option below. This is a convenience for setting a single header. | |
| Headers | map of strings to bytes | Map of strings to opaque bytes representing key-value pairs to set as headers on the message. This may overwrite previously set headers. | |
| Key | bytes | Opaque bytes used for the message key. If Liftbridge has stream compaction enabled, the stream will retain only the last message for each key (if not set, the message is always retained). | |
| PartitionByKey | bool | Flag which maps the message to a stream partition based on a hash of the message key. This computes the partition number for the message by hashing the key and modding by the number of partitions for the first stream found with the subject of the published message. This does not work with streams containing wildcards in their subjects, e.g. `foo.*`, since this matches on the subject literal of the published message. This also has undefined behavior if there are multiple streams for the given subject. This is used to derive the actual NATS subject the message is published to, e.g. `foo`, `foo.1`, `foo.2`, etc. By default, it's published to the subject provided. | false |
| PartitionByRoundRobin | bool | Flag which maps the message to stream partitions in a round-robin fashion. This computes the partition number for the message by atomically incrementing a counter for the message subject and modding it by the number of partitions for the first stream found with the subject. This does not work with streams containing wildcards in their subjects, e.g. `foo.*`, since this matches on the subject literal of the published message. This also has undefined behavior if there are multiple streams for the given subject. This is used to derive the actual NATS subject the message is published to, e.g. `foo`, `foo.1`, `foo.2`, etc. By default, it's published to the subject provided. | false |
| PartitionBy | partitioner | `Partitioner` (detailed below) which sets the strategy used to map the message to a stream partition. This is used to derive the actual NATS subject the message is published to, e.g. `foo`, `foo.1`, `foo.2`, etc. By default, it's published to the subject provided. | |
| ToPartition | int | Sets the partition to publish the message to. If this is set, any specified `Partitioner` will not be used. This is used to derive the actual NATS subject the message is published to, e.g. `foo`, `foo.1`, `foo.2`, etc. By default, it's published to the subject provided. | |

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

[Implementation Guidance](#publish-implementation)

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

Note that the envelope-cookie protocol does not need to be implemented in the
`Publish` API since the envelope serialization is handled by the server.

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

### FetchMetadata

```go
// FetchMetadata returns cluster metadata including broker and stream
// information.
func FetchMetadata(ctx context.Context) (*Metadata, error)
```

Liftbridge provides a metadata API which clients can use to retrieve cluster
metadata including server (broker) and stream information. There are a few key
use cases for this metadata:

- The client can connect to a single seed server, fetch the cluster metadata,
  and then populate a local cache of known servers in order to allow for
  failover of subsequent RPCs. This should be handled internally by client
  libraries.
- Subscribe requests must be sent to the leader of the requested stream
  partition, so the client can fetch metadata from the cluster to determine
  which server to send particular requests to. This should be handled
  internally by client libraries.
- Stream partitioning requires knowing how many partitions exist for a stream,
  so the client can fetch metadata from the cluster to determine partitions.
  This should be handled internally by client libraries for things such as
  `PartitionByKey` and `PartitionByRoundRobin` but may be handled by users for
  more advanced use cases.

`FetchMetadata` retrieves an immutable object containing this cluster metadata.
As noted in the above use cases, most uses of this metadata is handled
internally by client libraries. Generally, `FetchMetadata` is only needed by
users in cases where the number of stream partitions is required.

In the Go client example above, `FetchMetadata` takes one argument:

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. | language-dependent |

[Implementation Guidance](#fetchmetadata-implementation)

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

## Client Implementation

Below is implementation guidance for the client interface described above. Like
the interface, specific implementation details may differ depending on the
programming language.

### RPCs

While Liftbridge has a concept of a metadata leader, or controller, which is
responsible for handling updates to cluster metadata, most RPCs can be made to
any server in the cluster. The only exception to this is `Subscribe`, which
requires the RPC to be made to the leader of the partition being subscribed to.
Metadata RPCs will be automatically forwarded to the metadata leader in the
cluster.

With this in mind, we recommend implementing a reusable method for making
resilient RPCs by retrying requests on certain failures and cycling through
known servers. It's important to be mindful of which errors are retried for
idempotency reasons. This resilient RPC method can be used for RPCs such as:

- `CreateStream`
- `Publish`
- `FetchMetadata`

The Go implementation of this, called `doResilientRPC`, is shown below along
with the `dialBroker` helper. It takes an RPC closure and retries on gRPC
`Unavailable` errors while trying different servers in the cluster.
`dialBroker` relies on the internally managed [metadata
object](#fetchmetadata-implementation) to track server addresses in the
cluster.

```go
// doResilientRPC executes the given RPC and performs retries if it fails due
// to the broker being unavailable, cycling through the known broker list.
func (c *client) doResilientRPC(rpc func(client proto.APIClient) error) (err error) {
	c.mu.RLock()
	client := c.apiClient
	c.mu.RUnlock()

	for i := 0; i < 10; i++ {
		err = rpc(client)
		if status.Code(err) == codes.Unavailable {
			conn, err := c.dialBroker()
			if err != nil {
				return err
			}
			client = proto.NewAPIClient(conn)
			c.mu.Lock()
			c.apiClient = client
			c.conn.Close()
			c.conn = conn
			c.mu.Unlock()
		} else {
			break
		}
	}
	return
}

// dialBroker dials each broker in the cluster, in random order, returning a
// gRPC ClientConn to the first one that is successful.
func (c *client) dialBroker() (*grpc.ClientConn, error) {
	var (
		conn  *grpc.ClientConn
		err   error
		addrs = c.metadata.getAddrs()
		perm  = rand.Perm(len(addrs))
	)
	for _, i := range perm {
		conn, err = grpc.Dial(addrs[i], c.dialOpts...)
		if err == nil {
			break
		}
	}
	if conn == nil {
		return nil, err
	}
	return conn, nil
}
```

### Connection Pooling

A single client might have multiple connections to different servers in a
Liftbridge cluster or even multiple connections to the same server. For
efficiency purposes, it's recommended client libraries implement connection
pooling to reuse and limit gRPC connections. For simplicity, we recommend
having a dedicated connection for [all RPCs with the exception of
`Subscribe`](#rpcs) and using connection pooling for `Subscribe` RPCs which are
long-lived and asynchronous.

There are two important client options that control pooling behavior:
`KeepAliveTime` and `MaxConnsPerBroker`.  `KeepAliveTime` is the amount of time
a pooled connection can be idle (unused) before it is closed and removed from
the pool. A suggested default for this is 30 seconds. `MaxConnsPerBroker` is
the maximum number of connections to pool for a given server in the cluster. A
suggested default for this is 2.

Since a connection pool is generally scoped to a server, a client will
typically have a map of server addresses to connection pools.

A connection pool generally has three key operations:

- `get`: return a gRPC connection from the pool, if available, or create one
  using a provided connection factory. If the connection was taken from a pool,
  an expiration timer should be canceled on it to prevent it from being closed.
- `put`: return a gRPC connection to the pool if there is capacity or close it
  if not. If the connection is added to the pool, a timer should be set to
  close the connection if it's unused for `KeepAliveTime`.
- `close`: clean up the connection pool by closing all active gRPC connections
  and stopping all timers.

### CreateStream Implementation

The `CreateStream` implementation simply constructs a gRPC request and executes
it using the [resilient RPC method](#rpcs) described above. If the
`AlreadyExists` gRPC error is returned, an `ErrStreamExists` error/exception is
thrown. Otherwise, any other error/exception is thrown if the operation failed.

```go
// CreateStream creates a new stream attached to a NATS subject. Subject is the
// NATS subject the stream is attached to, and name is the stream identifier,
// unique per subject. It returns ErrStreamExists if a stream with the given
// subject and name already exists.
func (c *client) CreateStream(ctx context.Context, subject, name string, options ...StreamOption) error {
	opts := &StreamOptions{}
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return err
		}
	}

	req := &proto.CreateStreamRequest{
		Subject:           subject,
		Name:              name,
		ReplicationFactor: opts.ReplicationFactor,
		Group:             opts.Group,
		Partitions:        opts.Partitions,
	}
	err := c.doResilientRPC(func(client proto.APIClient) error {
		_, err := client.CreateStream(ctx, req)
		return err
	})
	if status.Code(err) == codes.AlreadyExists {
		return ErrStreamExists
	}
	return err
}
```

### Subscribe Implementation

`Subscribe` works by making a gRPC request which returns a stream that sends
back messages on a partition. As such, there are essentially two main
implementation components: making the subscribe request and starting a
dispatcher which asynchronously sends messages received on the gRPC stream to
the user, either using a callback or some other message-passing mechanism. The
Go implementation of this is shown below:

```go
// Subscribe creates an ephemeral subscription for the given stream. It begins
// receiving messages starting at the configured position and waits for new
// messages when it reaches the end of the stream. The default start position
// is the end of the stream. It returns an ErrNoSuchPartition if the given
// stream or partition does not exist. Use a cancelable Context to close a
// subscription.
func (c *client) Subscribe(ctx context.Context, streamName string, handler Handler,
	options ...SubscriptionOption) (err error) {

	opts := &SubscriptionOptions{}
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return err
		}
	}

	stream, releaseConn, err := c.subscribe(ctx, streamName, opts)
	if err != nil {
		return err
	}

	go c.dispatchStream(ctx, streamName, stream, releaseConn, handler)
	return nil
}
```

The `Subscribe` RPC must be sent to the leader of the stream partition being
subscribed to. This should be determined by [fetching the
metadata](#fetchmetadata) from the cluster. This metadata can (and should) be
cached in the client. It's recommended `Subscribe` use [connection
pooling](#connection-pooling) since each `Subscribe` call will involve a
long-lived gRPC connection to a different server.

When the subscription stream is created, the server sends an empty message to
indicate the subscription was successfully created. Otherwise, an error is sent
on the stream if the subscribe failed. A gRPC `FailedPrecondition` error
indicates the server is not the partition leader, perhaps because the leader
has since changed. In this case, the client should refresh the metadata and
retry. It's recommended retries also wait a bit, e.g. between 10 and 500 ms,
ideally with some jitter. A gRPC `NotFound` error is returned if the requested
partition does not exist.

After the subscription is created and the server has returned a gRPC stream for
the client to receive messages on, `Subscribe` should start an asynchronous
thread, coroutine, or equivalent to send messages to the user. For example,
this might invoke a callback or place messages in a buffer. Alternatively,
`Subscribe` may be blocking, in which case it simply yields a stream of
messages to the user in a synchronous manner. There are a few important pieces
of behavior to point out with the dispatching component:

- If an error is received on the gRPC stream, it should be propagated to the
  user. This should signal that the subscription is terminated and no more
  messages are to be received.
- If an error indicating a transient network issue occurs, e.g. a gRPC
  `Unavailable` error, the client may opt to resubscribe to the partition
  starting at the last received offset. It's recommended to provide a
  configuration, `ResubscribeWaitTime`, which is the amount of time to attempt
  to re-establish a subscription after being disconnected. A suggested default
  for this is 30 seconds. It's recommended to have some wait time with jitter
  between resubscribe attempts. This resubscribe logic implies `Subscribe` is
  tracking the last received offset from the partition in order to know where
  to start a resubscribe from. Note that the internal resubscribe logic is
  purely best effort and, at the moment, it's up to users to track their
  position in the stream if needed.
- When a subscription is closed, either explicitly or due to an error, its
  connection should be returned to the connection pool.

### Publish Implementation

`Publish` involves constructing a `Message` protobuf, determining the partition
to publish to, and then making the publish request to the server using the
[resilient RPC method](#rpcs) described above. The Go implementation of this is
shown below, including a helper function `partition` which determines the
partition ID to publish the message to.

```go
// Publish publishes a new message to the NATS subject. If the AckPolicy is not
// NONE and a deadline is provided, this will synchronously block until the
// first ack is received. If the ack is not received in time, a
// DeadlineExceeded status code is returned. If an AckPolicy and deadline are
// configured, this returns the first Ack on success, otherwise it returns nil.
func (c *client) Publish(ctx context.Context, subject string, value []byte,
	options ...MessageOption) (*proto.Ack, error) {

	opts := &MessageOptions{Headers: make(map[string][]byte)}
	for _, opt := range options {
		opt(opts)
	}

	msg := &proto.Message{
		Subject:       subject,
		Key:           opts.Key,
		Value:         value,
		AckInbox:      opts.AckInbox,
		CorrelationId: opts.CorrelationID,
		AckPolicy:     opts.AckPolicy,
	}

	// Determine which partition to publish to.
	partition, err := c.partition(ctx, msg, opts)
	if err != nil {
		return nil, err
	}

	// Publish to the appropriate partition subject.
	if partition > 0 {
		msg.Subject = fmt.Sprintf("%s.%d", msg.Subject, partition)
	}

	var (
		req = &proto.PublishRequest{Message: msg}
		ack *proto.Ack
	)
	err = c.doResilientRPC(func(client proto.APIClient) error {
		resp, err := client.Publish(ctx, req)
		if err == nil {
			ack = resp.Ack
		}
		return err
	})
	return ack, err
}

// partition determines the partition ID to publish the message to. If a
// partition was explicitly provided, it will be returned. If a Partitioner was
// provided, it will be used to compute the partition. Otherwise, 0 will be
// returned.
func (c *client) partition(ctx context.Context, msg *proto.Message, opts *MessageOptions) (int32, error) {
	var partition int32
	// If a partition is explicitly provided, use it.
	if opts.Partition != nil {
		partition = *opts.Partition
	} else if opts.Partitioner != nil {
		// Make sure we have metadata for the stream and, if not, update it.
		metadata, err := c.waitForSubjectMetadata(ctx, msg.Subject)
		if err != nil {
			return 0, err
		}
		partition = opts.Partitioner.Partition(msg, metadata)
	}
	return partition, nil
}
```

The partition to publish to is determined in the following way:

1. If a partition is explicitly provided using the `ToPartition` message
   option, use it.
2. If a `Partitioner` is provided, use it to compute the partition.
3. If neither of the above apply, use partition 0 (the default partition).

The partition is used to derive the actual NATS subject the message is
published to. The table below shows an example of this partition-subject
mapping logic.

| Base Subject | Partition | Derived Subject |
|:----|:----|:----|
| foo | 0 | foo |
| foo | 1 | foo.1 |
| foo | 2 | foo.2 |
| foo | 3 | foo.3 |

Note that `Publish` is a synchronous operation. After the RPC returns, the
message has been published. The server handles message acks, if applicable. If
a publish is expecting an ack, the RPC will block until the ack is received or
a timeout occurs. Implementations may choose to also offer an asynchronous
publish API for performance reasons. The Go client does not currently implement
this.

#### Partitioner Implementation

Clients should provide at least two `Partitioner` implementations as described
above: `PartitionByKey` and `PartitionByRoundRobin`. `PartitionByKey` should
partition messages based on a hash of the message key. `PartitionByRoundRobin`
should partition messages in a round-robin fashion, i.e. cycling through the
partitions. The Go implementations of these are shown below:

```go
var hasher = crc32.ChecksumIEEE

// keyPartitioner is an implementation of Partitioner which partitions messages
// based on a hash of the key.
type keyPartitioner struct{}

// Partition computes the partition number for a given message by hashing the
// key and modding by the number of partitions for the first stream found with
// the subject of the message. This does not work with streams containing
// wildcards in their subjects, e.g. "foo.*", since this matches on the subject
// literal of the published message. This also has undefined behavior if there
// are multiple streams for the given subject.
func (k *keyPartitioner) Partition(msg *proto.Message, metadata *Metadata) int32 {
	key := msg.Key
	if key == nil {
		key = []byte("")
	}

	partitions := getPartitionCount(msg.Subject, metadata)
	if partitions == 0 {
		return 0
	}

	return int32(hasher(key)) % partitions
}

type subjectCounter struct {
	sync.Mutex
	count int32
}

// roundRobinPartitioner is an implementation of Partitioner which partitions
// messages in a round-robin fashion.
type roundRobinPartitioner struct {
	sync.Mutex
	subjectCounterMap map[string]*subjectCounter
}

// Partition computes the partition number for a given message in a round-robin
// fashion by atomically incrementing a counter for the message subject and
// modding by the number of partitions for the first stream found with the
// subject. This does not work with streams containing wildcards in their
// subjects, e.g. "foo.*", since this matches on the subject literal of the
// published message. This also has undefined behavior if there are multiple
// streams for the given subject.
func (r *roundRobinPartitioner) Partition(msg *proto.Message, metadata *Metadata) int32 {
	partitions := getPartitionCount(msg.Subject, metadata)
	if partitions == 0 {
		return 0
	}
	r.Lock()
	counter, ok := r.subjectCounterMap[msg.Subject]
	if !ok {
		counter = new(subjectCounter)
		r.subjectCounterMap[msg.Subject] = counter
	}
	r.Unlock()
	counter.Lock()
	count := counter.count
	counter.count++
	counter.Unlock()
	return count % partitions
}

func getPartitionCount(subject string, metadata *Metadata) int32 {
	counts := metadata.PartitionCountsForSubject(subject)

	// Get the first matching stream's count.
	for _, count := range counts {
		return count
	}

	return 0
}
```

### FetchMetadata Implementation

`FetchMetadata` should return an immutable object which exposes cluster
metadata. This object is passed in to `Partitioner` implementations, but users
can also use it to retrieve stream and server information. Below are the
function signatures for the Go implementation of this object. This may vary
between language implementations.

```go
// LastUpdated returns the time when this metadata was last updated from the
// server.
func (m *Metadata) LastUpdated() time.Time {}

// Brokers returns a list of the cluster nodes.
func (m *Metadata) Brokers() []*BrokerInfo {}

// Addrs returns the list of known broker addresses.
func (m *Metadata) Addrs() []string {}

// GetStream returns the given stream or nil if unknown.
func (m *Metadata) GetStream(name string) *StreamInfo {}

// GetStreams returns a map containing all streams with the given subject. This
// does not match on wildcard subjects, e.g.  "foo.*".
func (m *Metadata) GetStreams(subject string) []*StreamInfo {}

// PartitionCountsForSubject returns a map containing stream names and the
// number of partitions for the stream. This does not match on wildcard
// subjects, e.g. "foo.*".
func (m *Metadata) PartitionCountsForSubject(subject string) map[string]int32 {}
```

`BrokerInfo` is an immutable object containing server information like ID,
host, and port. `StreamInfo` is an immutable object containing stream
information like subject, name, and partitions.

To simplify client logic, we recommend building an internal abstraction for
dealing with metadata. This internal API should include logic for fetching
metadata from the cluster using the [resilient RPC method](#rpcs) described
above, converting it into the immutable metadata object, and caching it. It
should also provide helper methods for retrieving server addresses (all
addresses, which helps simplify logic for resilient RPCs, and the address for
the leader of a given stream partition, which helps simplify logic for
`Subscribe`).

With this internal metadata abstraction, the implementation of `FetchMetadata`
is should be trivial, as shown in the Go implementation below. In this case,
the logic of making the metadata RPC, converting it into an immutable metadata
object, and caching it is handled by the `update` function.

```go
// FetchMetadata returns cluster metadata including broker and stream
// information.
func (c *client) FetchMetadata(ctx context.Context) (*Metadata, error) {
	return c.metadata.update(ctx)
}
```

While metadata should be cached internally to minimize RPCs, the public
`FetchMetadata` should always force a refresh by going to the server.
Implementations may choose to provide an option for using the cached metadata.
Internally, calls such as `Subscribe` should attempt to use the cached metadata
and refresh it if it appears to be stale, e.g. because the server is no longer
the partition leader. It may also be prudent for clients to periodically
refresh metadata irrespective of these types of errors, perhaps with a
configurable interval. However, the Go client does not currently implement
this.

### Close Implementation

`Close` should be an idempotent operation which closes any gRPC connections and
connection pools associated with the client. The Go implementation of this is
shown below:

```go
// Close the client connection.
func (c *client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	for _, pool := range c.pools {
		if err := pool.close(); err != nil {
			return err
		}
	}
	if err := c.conn.Close(); err != nil {
		return err
	}
	c.closed = true
	return nil
}
```
