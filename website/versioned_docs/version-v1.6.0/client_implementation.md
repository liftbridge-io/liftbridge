---
id: version-v1.6.0-client-implementation
title: Client Implementation Guidance
original_id: client-implementation
---

This documentation provides guidance on implementing a [client
library](clients.md) for the Liftbridge server. If information is missing or
inaccurate, feel free to make a PR fixing it.

## Generating gRPC API

Liftbridge relies on gRPC and Protocol Buffers (protobufs) for its API. These
definitions are stored in the
[liftbridge-api](https://github.com/liftbridge-io/liftbridge-api) repository.
Refer to the [gRPC documentation](https://grpc.io/docs/) for guidance on
generating the gRPC code from the protobuf definitions for your particular
programming language. The liftbridge-api repository also contains generated
code for some languages which you can use in your client library (for example,
the official Go client [relies on](https://github.com/liftbridge-io/go-liftbridge/blob/6fbf530bb220797fd91174d9f858fad3114dbc48/go.mod#L8)
the generated Go code in liftbridge-api). If there is no generated code for
your language, feel free to make a PR adding it. Otherwise, you can simply
vendor the generated code alongside your client library.

Generating the gRPC API results in a low-level client that can be used to talk
to Liftbridge. However, it's useful to implement a higher-level client that
provides a more user-friendly abstraction. The remainder of this guide relates
to implementing a high-level client.

## Client Interface

A high-level client has the following operations:

| Operation | Description |
|:----|:----|
| [CreateStream](#createstream) | Creates a new stream attached to a NATS subject (or group of related NATS subjects if partitioned) |
| [DeleteStream](#deletestream) | Deletes a stream and all of its partitions |
| [PauseStream](#pausestream) | Pauses some or all of a stream's partitions until they are published to |
| [SetStreamReadonly](#setstreamreadonly) | Sets some or all of a stream's partitions as readonly or readwrite |
| [Subscribe](#subscribe) | Creates an ephemeral subscription for a given stream that messages are received on |
| [Publish](#publish) | Publishes a new message to a Liftbridge stream |
| [PublishAsync](#publishasync) | Publishes a new message to a Liftbridge stream asynchronously |
| [PublishToSubject](#publishtosubject) | Publishes a new message to a NATS subject |
| [FetchMetadata](#fetchmetadata) | Retrieves metadata from the cluster |
| [FetchPartitionMetadata](#fetchpartitionmetadata) | Retrieves partition metadata from the partition leader |
| [SetCursor](#setcursor) | Persists a cursor position for a particular stream partition. |
| [FetchCursor](#fetchcursor) | Retrieves a cursor position for a particular stream partition. |
| [Close](#close) | Closes any client connections to Liftbridge |

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

	// DeleteStream deletes a stream and all of its partitions. Name is the
	// stream identifier, globally unique.
	DeleteStream(ctx context.Context, name string) error

	// PauseStream pauses a stream and some or all of its partitions. Name is
	// the stream identifier, globally unique. It returns an ErrNoSuchPartition
	// if the given stream or partition does not exist. By default, this will
	// pause all partitions. A partition is resumed when it is published to via
	// the Liftbridge Publish API or ResumeAll is enabled and another partition
	// in the stream is published to.
	PauseStream(ctx context.Context, name string, opts ...PauseOption) error

	// SetStreamReadonly sets the readonly flag on a stream and some or all of
	// its partitions. Name is the stream identifier, globally unique. It
	// returns an ErrNoSuchPartition if the given stream or partition does not
	// exist. By default, this will set the readonly flag on all partitions.
	// Subscribers to a readonly partition will see their subscription ended
	// with a ErrReadonlyPartition error once all messages currently in the
	// partition have been read.
	SetStreamReadonly(ctx context.Context, name string, opts ...ReadonlyOption) error

	// Subscribe creates an ephemeral subscription for the given stream. It
	// begins receiving messages starting at the configured position and waits
	// for new messages when it reaches the end of the stream. The default
	// start position is the end of the stream. It returns an
	// ErrNoSuchPartition if the given stream or partition does not exist. Use
	// a cancelable Context to close a subscription.
	Subscribe(ctx context.Context, stream string, handler Handler, opts ...SubscriptionOption) error

	// Publish publishes a new message to the Liftbridge stream. The partition
	// that gets published to is determined by the provided partition or
	// Partitioner passed through MessageOptions, if any. If a partition or
	// Partitioner is not provided, this defaults to the base partition. This
	// partition determines the underlying NATS subject that gets published to.
	// To publish directly to a specific NATS subject, use the low-level
	// PublishToSubject API.
	//
	// If the AckPolicy is not NONE, this will synchronously block until the
	// ack is received. If the ack is not received in time, ErrAckTimeout is
	// returned. If AckPolicy is NONE, this returns nil on success.
	Publish(ctx context.Context, stream string, value []byte, opts ...MessageOption) (*Ack, error)

	// PublishAsync publishes a new message to the Liftbridge stream and
	// asynchronously processes the ack or error for the message.
	PublishAsync(ctx context.Context, stream string, value []byte, ackHandler AckHandler, opts ...MessageOption) error

	// PublishToSubject publishes a new message to the NATS subject. Note that
	// because this publishes directly to a subject, there may be multiple (or
	// no) streams that receive the message. As a result, MessageOptions
	// related to partitioning will be ignored. To publish at the
	// stream/partition level, use the high-level Publish API.
	//
	// If the AckPolicy is not NONE and a deadline is provided, this will
	// synchronously block until the first ack is received. If an ack is not
	// received in time, ErrAckTimeout is returned. If an AckPolicy and
	// deadline are configured, this returns the first Ack on success,
	// otherwise it returns nil.
	PublishToSubject(ctx context.Context, subject string, value []byte, opts ...MessageOption) (*Ack, error)

	// FetchMetadata returns cluster metadata including broker and stream
	// information.
	FetchMetadata(ctx context.Context) (*Metadata, error)

	// SetCursor persists a cursor position for a particular stream partition.
	// This can be used to checkpoint a consumer's position in a stream to
	// resume processing later.
	SetCursor(ctx context.Context, id, stream string, partition int32, offset int64) error

	// FetchCursor retrieves a cursor position for a particular stream
	// partition. It returns -1 if the cursor does not exist.
	FetchCursor(ctx context.Context, id, stream string, partition int32) (int64, error)
}
```

### CreateStream

```go
// CreateStream creates a new stream attached to a NATS subject. Subject is
// the NATS subject the stream is attached to, and name is the stream
// identifier, unique per subject. It returns ErrStreamExists if a stream
// with the given subject and name already exists.
func CreateStream(ctx context.Context, subject, name string, opts ...StreamOption) error
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
| RetentionMaxBytes | int64 | The maximum size a stream's log can grow to, in bytes, before we will discard old log segments to free up space. A value of 0 indicates no limit. If this is not set, it takes the server default. |  |
| RetentionMaxMessages | int64 | The maximum size a stream's log can grow to, in number of messages, before we will discard old log segments to free up space. A value of 0 indicates no limit. If this is not set, it takes the server default. |  |
| RetentionMaxAge | time duration | The TTL for stream log segment files, after which they are deleted. A value of 0 indicates no TTL. If this is not set, it takes the server default.  |  |
| CleanerInterval | time duration | The frequency to check if a new stream log segment file should be rolled and whether any segments are eligible for deletion based on the retention policy or compaction if enabled. If this is not set, it takes the server default. |  |
| SegmentMaxBytes | int64 | The maximum size of a single stream log segment file in bytes. Retention is always done a file at a time, so a larger segment size means fewer files but less granular control over retention. If this is not set, it takes the server default. |  |
| SegmentMaxAge | time duration | The maximum time before a new stream log segment is rolled out. A value of 0 means new segments will only be rolled when segment.max.bytes is reached. Retention is always done a file at a time, so a larger value means fewer files but less granular control over retention. If this is not set, it takes the server default. |  |
| CompactMaxGoroutines | int32 | The maximum number of concurrent goroutines to use for compaction on a stream log (only applicable if compact.enabled is true). If this is not set, it takes the server default. |  |
| CompactEnabled | bool | Enable message compaction by key on the server for this stream. If this is not set, it takes the server default. |  |

`CreateStream` returns/throws an error if the operation fails, specifically
`ErrStreamExists` if a stream with the given name already exists.

[Implementation Guidance](#createstream-implementation)

### DeleteStream

```go
// DeleteStream deletes a stream and all of its partitions. Name is the stream
// identifier, globally unique.
func (c *client) DeleteStream(ctx context.Context, name string) error
```

`DeleteStream` deletes a stream and all of its partitions. This will remove any
data stored on disk for the stream and all of its partitions.

In the Go client example above, `DeleteStream` takes two arguments:

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. | language-dependent |
| name | string | The name of the stream to delete. | yes |

`DeleteStream` returns/throws an error if the operation fails, specifically
`ErrNoSuchStream` if the stream doesn't exist.

[Implementation Guidance](#deletestream-implementation)

### PauseStream

```go
// PauseStream pauses a stream and some or all of its partitions. Name is the
// stream identifier, globally unique. It returns an ErrNoSuchPartition if the
// given stream or partition does not exist. By default, this will pause all
// partitions. A partition is resumed when it is published to via the
// Liftbridge Publish API or ResumeAll is enabled and another partition in the
// stream is published to.
func (c *client) PauseStream(ctx context.Context, name string, options ...PauseOption)
```

`PauseStream` pauses some or all of a stream's partitions. In effect, thie
means closing NATS subscriptions, stopping replication, closing any associated
file handles, and generally releasing any resources used in maintaining a
partition. This can be done selectively on partitions or the entirety of a
stream.

A partition is paused until it is published to via the Liftbridge
[`Publish`](#publish) API or `ResumeAll` is enabled and another partition in
the stream is published to.

Different sets of partitions can be paused independently with subsequent
`PauseStream` requests to the same stream, and pausing partitions is an
idempotent operation. However, note that the stream will be updated with the
latest `ResumeAll` value specified on the request.

In the Go client example above, `PauseStream` takes three arguments:

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. | language-dependent |
| name | string | The name of the stream to pause partitions for. | yes |
| options | pause options | Zero or more pause options. These are used to pass in optional settings for pausing the stream. This is a common [Go pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) for implementing extensible APIs. In other languages, this might be replaced with a builder pattern or optional named arguments. These `PauseStream` options are described below. | language-dependent |

The pause options are the equivalent of optional named arguments used to
configure stream pausing. Supported options are:

| Option | Type | Description | Default |
|:----|:----|:----|:----|
| PausePartitions | list of ints | Specifies the stream partitions to pause. If not set, all partitions will be paused. | |
| ResumeAll | bool | Resume all partitions in the stream if any are published to instead of resuming only the partition published to. | false |

`PauseStream` returns/throws an error if the operation fails, specifically
`ErrNoSuchPartition` if the stream or partition(s) do not exist.

[Implementation Guidance](#pausestream-implementation)

### SetStreamReadonly

```go
// SetStreamReadonly sets the readonly flag on a stream and some or all of
// its partitions. Name is the stream identifier, globally unique. It
// returns an ErrNoSuchPartition if the given stream or partition does not
// exist. By default, this will set the readonly flag on all partitions.
// Subscribers to a readonly partition will see their subscription ended
// with a ErrReadonlyPartition error once all messages currently in the
// partition have been read.
func (c *client) SetStreamReadonly(ctx context.Context, name string, options ...ReadonlyOption) error
```

`SetStreamReadonly` sets some or all of a stream's partitions as readonly or
readwrite. Attempting to publish to a readonly partition will result in an
`ErrReadonlyPartition` error/exception. Subscribers consuming a readonly
partition will read up to the end of the partition and then receive an
`ErrReadonlyPartition` error.

In the Go client example above, `SetStreamReadonly` takes three arguments:

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. | language-dependent |
| name | string | The name of the stream whose partitions to set the readonly flag for. | yes |
| options | readonly options | Zero or more readonly options. These are used to pass in optional settings for setting the readonly flag. This is a common [Go pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) for implementing extensible APIs. In other languages, this might be replaced with a builder pattern or optional named arguments. These `SetStreamReadonly` options are described below. | language-dependent |

The readonly options are the equivalent of optional named arguments used to
configure readonly requests. Supported options are:

| Option | Type | Description | Default |
|:----|:----|:----|:----|
| ReadonlyPartitions | list of ints | Specifies the stream partitions to set the readonly flag for. If not set, all partitions will be changed. | |
| Readonly | bool | Defines if the partitions should be set to readonly or readwrite. | false |

`SetStreamReadonly` returns/throws an error if the operation fails,
specifically `ErrNoSuchPartition` if the stream or partition(s) do not exist.

[Implementation Guidance](#setstreamreadonly-implementation)

### Subscribe

```go
// Subscribe creates an ephemeral subscription for the given stream. It
// begins receiving messages starting at the configured position and waits
// for new messages when it reaches the end of the stream. The default
// start position is the end of the stream. It returns an ErrNoSuchStream
// if the given stream does not exist. Use a cancelable Context to close a
// subscription.
func Subscribe(ctx context.Context, stream string, handler Handler, opts ...SubscriptionOption) error
```

`Subscribe` is used to consume streams. By default, it begins reading messages
from the end of the stream and blocks for new messages.

By default, the client automatically binds to the partition's leader for a
subscribe request. However, it is also possible to bind a subscribe request to
a random ISR replica, including partition followers, with an additional opt-in
flag. Like the leader, an ISR follower will only expose committed messages.
However, there may be some lag behind what is committed on the leader before
it's committed on a follower.

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
callback. See [below](#messages-and-acks) for guidance on implementing
messages.

```go
// Handler is the callback invoked by Subscribe when a message is received on
// the specified stream. If err is not nil, the subscription will be terminated
// and no more messages will be received.
type Handler func(msg *Message, err error)
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
| ReadISRReplica | bool | Sets the subscription to one of a random ISR replica instead of subscribing to the partition's leader. | false |
| Resume | bool | Specifies whether a paused partition should be resumed before subscribing. | false |

Currently, `Subscribe` can only subscribe to a single partition. In the future,
there will be functionality for consuming all partitions.

`Subscribe` returns/throws an error if the operation fails, specifically
`ErrNoSuchPartition` if the specified stream or partition does not exist.

[Implementation Guidance](#subscribe-implementation)

### Publish

```go
// Publish publishes a new message to the Liftbridge stream. The partition
// that gets published to is determined by the provided partition or
// Partitioner passed through MessageOptions, if any. If a partition or
// Partitioner is not provided, this defaults to the base partition. This
// partition determines the underlying NATS subject that gets published to.
// To publish directly to a specific NATS subject, use the low-level
// PublishToSubject API.
//
// If the AckPolicy is not NONE, this will synchronously block until the
// ack is received. If the ack is not received in time, ErrAckTimeout is
// returned. If AckPolicy is NONE, this returns nil on success.
Publish(ctx context.Context, stream string, value []byte, opts ...MessageOption) (*Ack, error)
```

`Publish` sends a message to a Liftbridge stream. Since Liftbridge streams
attach to normal NATS subjects, it's also possible to [publish messages
directly to NATS](https://github.com/liftbridge-io/go-liftbridge#publishing-directly-with-nats)
using a [NATS client](https://nats.io/download/). Liftbridge works fine with
plain, opaque NATS messages, but it also extends NATS with a protobuf-based
[envelope protocol](./envelope_protocol.md). This allows publishers to add
metadata to messages like a key, headers, and acking information. Liftbridge
client libraries may provide helper methods to make it easy to create envelopes
and deal with acks yourself using a NATS client directly ([described
below](#low-level-publish-helpers)). However, the `Publish` API is intended to
abstract this work away from you.

`Publish` is a synchronous operation, meaning when it returns, the message has
been successfully published. `Publish` can also be configured to block until a
message acknowledgement (ack) is returned from the cluster. This is useful for
ensuring a message has been stored and replicated, guaranteeing at-least-once
delivery. The default ack policy is `LEADER`, meaning the ack is sent once the
partition leader has stored the message. See [below](#messages-and-acks) for
guidance on implementing acks.

If the ack policy is not `NONE`, `Publish` will synchronously block until the
ack is received. If the ack is not received in time, an `ErrAckTimeout`
error/exception is thrown. If the ack policy is `NONE`, this returns
immediately.

`Publish` can send messages to particular stream partitions using a
`Partitioner` or an explicitly provided partition. By default, it publishes to
the base partition. The partition determines the underlying NATS subject that
gets published to. To publish directly to a specific NATS subject, use the
low-level [`PublishToSubject`](#publishtosubject) API.

In the Go client example above, `Publish` takes four arguments:

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. With `Publish`, the timeout is particularly important as it relates to acking. If an ack policy is set (see below) and a timeout is provided, `Publish` will block until the first ack is received. If the ack is not received in time, a timeout error is returned. If the ack policy is `NONE` or a timeout is not set, `Publish` returns as soon as the message has been published. | language-dependent |
| stream | string | The stream to publish to. | yes |
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
	Partition(stream string, key, value []byte, metadata *Metadata) int32
}
```

[Implementation Guidance](#publish-implementation)

### PublishAsync

```go
// PublishAsync publishes a new message to the Liftbridge stream and
// asynchronously processes the ack or error for the message.
PublishAsync(ctx context.Context, stream string, value []byte, ackHandler AckHandler, opts ...MessageOption) error
```

`PublishAsync` sends a message to a Liftbridge stream asynchronously. This is
similar to [`Publish`](#publish), but rather than waiting for the ack, it
dispatches the ack with an ack handler callback. 

If the ack policy is not `NONE`, `PublishAsync` will asynchronously dispatch
the ack when it is received. If the ack is not received in time, an
`ErrAckTimeout` error/exception is dispatched. If the ack policy is `NONE`, the
callback will not be invoked. If no ack handler is provided, any ack will be
ignored.

In the Go client example above, `PublishAsync` takes five arguments:

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. With `PublishAsync`, the timeout is particularly important as it relates to acking. If an ack policy is set (see below) and a timeout is provided, `PublishAsync` will allow up to this much time to pass until the ack is received. If the ack is not received in time, a timeout error is returned. | language-dependent |
| stream | string | The stream to publish to. | yes |
| value | bytes | The message value to publish consisting of opaque bytes. | yes |
| ackHandler | callback | A handler callback used to receive the ack for the published message, described below. | yes |
| options | message options | Zero or more message options. These are used to pass in optional settings for the message. This is a common [Go pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) for implementing extensible APIs. In other languages, this might be replaced with a builder pattern or optional named arguments. These `PublishAsync` options are described below. | language-dependent |

The ack handler is a callback function that takes an ack and an error. If the
error is not null, this indicates the message was not successfully acked and
the corresponding ack argument will be null. The Go signature of the ack
handler callback is detailed below. In other languages, this might be a stream
mechanism instead of a callback. See [below](#messages-and-acks) for guidance
on implementing acks.

```go
// AckHandler is used to handle the results of asynchronous publishes to a
// stream. If the AckPolicy on the published message is not NONE, the handler
// will receive the ack once it's received from the cluster or an error if the
// message was not received successfully.
type AckHandler func(ack *Ack, err error)
```

The `PublishAsync` message options are the equivalent of optional named
arguments used to configure a message. Supported options are the same as those
supported by [`Publish`](#publish).

[Implementation Guidance](#publishasync-implementation)

### PublishToSubject

```go
// PublishToSubject publishes a new message to the NATS subject. Note that
// because this publishes directly to a subject, there may be multiple (or
// no) streams that receive the message. As a result, MessageOptions
// related to partitioning will be ignored. To publish at the
// stream/partition level, use the high-level Publish API.
//
// If the AckPolicy is not NONE and a deadline is provided, this will
// synchronously block until the first ack is received. If an ack is not
// received in time, ErrAckTimeout is returned. If an AckPolicy and
// deadline are configured, this returns the first Ack on success,
// otherwise it returns nil.
PublishToSubject(ctx context.Context, subject string, value []byte, opts ...MessageOption) (*Ack, error)
```

`PublishToSubject` sends a message to a NATS subject (and, in turn, any streams
that match the subject). Since Liftbridge streams attach to normal NATS
subjects, it's also possible to [publish messages directly to
NATS](https://github.com/liftbridge-io/go-liftbridge#publishing-directly-with-nats)
using a [NATS client](https://nats.io/download/). Liftbridge works fine with
plain, opaque NATS messages, but it also extends NATS with a protobuf-based
[envelope protocol](./envelope_protocol.md). This allows publishers to add
metadata to messages like a key, headers, and acking information. Liftbridge
client libraries may provide helper methods to make it easy to create envelopes
and deal with acks yourself using a NATS client directly ([described
below](#low-level-publish-helpers)). However, the `PublishToSubject` API is
intended to abstract this work away from you.

`PublishToSubject` is a synchronous operation, meaning when it returns, the
message has been successfully published. `PublishToSubject` can also be
configured to block until a message acknowledgement (ack) is returned from the
cluster. This is useful for ensuring a message has been stored and replicated,
guaranteeing at-least-once delivery. The default ack policy is `LEADER`,
meaning the ack is sent once the partition leader has stored the message.
`PublishToSubject` only waits on the first ack received. This is important to
be aware of since there may be multiple streams attached to a subject. See
[below](#messages-and-acks) for guidance on implementing acks.

In the Go client example above, `PublishToSubject` takes four arguments:

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. With `PublishToSubject`, the timeout is particularly important as it relates to acking. If an ack policy is set (see below) and a timeout is provided, `PublishToSubject` will block until the first ack is received. If the ack is not received in time, a timeout error is returned. If the ack policy is `NONE` or a timeout is not set, `PublishToSubject` returns as soon as the message has been published. | language-dependent |
| subject | string | The NATS subject to publish to. | yes |
| value | bytes | The message value to publish consisting of opaque bytes. | yes |
| options | message options | Zero or more message options. These are used to pass in optional settings for the message. This is a common [Go pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) for implementing extensible APIs. In other languages, this might be replaced with a builder pattern or optional named arguments. These `PublishToSubject` options are described below. | language-dependent |

The publish message options are the equivalent of optional named arguments used
to configure a message. Unlike `Publish`, `PublishToSubject` does not support
message options relating to partitioning since it operates at a lower level.
Supported options are:

| Option | Type | Description | Default |
|:----|:----|:----|:----|
| AckInbox | string | Sets the NATS subject Liftbridge should publish the message ack to. If this is not set, the server will generate a random inbox. This generally does not need to be configured when using the `PublishToSubject` API since the server will handle acks for you. Instead, it's used if, for some reason, you wish to handle the ack yourself. | |
| CorrelationID | string | Sets the identifier used to correlate an ack with the published message. If it's not set, the ack will not have a correlation ID. This generally does not need to be configured when using the `PublishToSubject` API since the server will handle acks for you. Instead, it's used if, for some reason, you wish to handle the ack yourself. | |
| AckPolicyAll | bool | Sets the ack policy of the message to `ALL`. This means the ack will be sent when the message has been stored by all partition replicas. | false |
| AckPolicyLeader | bool | Sets the ack policy of the message to `LEADER`. This means the ack will be sent when the partition leader has stored the message. This is the default ack policy if not otherwise set. | true |
| AckPolicyNone | bool | Sets the ack policy of the message to `NONE`. This means no ack will be sent. | false |
| Header | string and bytes | String and opaque bytes representing a key-value pair to set as a header on the message. This may overwrite previously set headers. Client libraries may choose to forgo this option in favor of the bulk `Headers` option below. This is a convenience for setting a single header. | |
| Headers | map of strings to bytes | Map of strings to opaque bytes representing key-value pairs to set as headers on the message. This may overwrite previously set headers. | |
| Key | bytes | Opaque bytes used for the message key. If Liftbridge has stream compaction enabled, the stream will retain only the last message for each key (if not set, the message is always retained). | |

[Implementation Guidance](#publishtosubject-implementation)

#### Low-Level Publish Helpers

Because Liftbridge is simply a consumer of NATS, it's also possible to publish
messages directly to NATS rather than using Liftbridge's `Publish` and
`PublishToSubject` APIs. This can be useful for extending the capabilities of
NATS in existing systems. This means we can publish NATS messages as normal, as
shown in the example Go code below, and Liftbridge streams will be able to
receive them.

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
capabilities of Liftbridge provided by message
[envelopes](./envelope_protocol.md), such as message headers, keys, etc. As a
result, client libraries may provide helper methods to facilitate publishing
message envelopes directly to NATS as well as handling acks. These include
`NewMessage`, `UnmarshalAck`, and `UnmarshalMessage` described below.

##### NewMessage

```go
// NewMessage returns a serialized message for the given payload and options.
func NewMessage(value []byte, options ...MessageOption) []byte
```

`NewMessage` creates a Liftbridge message envelope serialized to bytes ready
for publishing to NATS. This consists of an [envelope
header](envelope_protocol.md#liftbridge-envelope-header) followed by the
serialized message protobuf. It takes the same arguments as `Publish` (see
above) with the exception of the context and subject.

Note that the envelope protocol does not need to be implemented in the
`Publish` API since the envelope serialization is handled by the server.

##### UnmarshalMessage

```go
// UnmarshalMessage deserializes a message from the given byte slice. It
// returns an error if the given data is not actually a Message.
func UnmarshalMessage(data []byte) (Message, error)
```

`UnmarshalMessage` is a helper method which effectively does the reverse of
`NewMessage`, taking a serialized message envelope and returning a deserialized
message object or indication the data is not actually a message.

##### UnmarshalAck

```go
// UnmarshalAck deserializes an Ack from the given byte slice. It returns an
// error if the given data is not actually an Ack.
func UnmarshalAck(data []byte) (*Ack, error)
```

`UnmarshalAck` is used to deserialize a message ack received on a NATS
subscription. It takes a single argument consisting of the ack bytes as
received from NATS and throws an error/exception if the bytes are not actually
a serialized ack envelope.

This is useful for handling acks for messages published with a set ack inbox
(i.e. the NATS subject Liftbridge will publish message acks to). For example:

```go
// Connect to NATS.
nc, _ := nats.Connect(nats.DefaultURL)

// Subscribe to ack inbox.
nc.Subscribe("my.ack.inbox", func(msg *nats.Msg) {
    ack, _ := lift.UnmarshalAck(msg.Data)
    // CorrelationId can be used to correlate an ack with a particular message.
    if ack.CorrelationID() == "123" {
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

### FetchPartitionMetadata

```go
// FetchPartitionMetadata retrieves the latest partition metadata from partition leader
// The main interest is to retrieve Highest Watermark and Newest Offset
FetchPartitionMetadata(ctx context.Context, stream string, partition int32) (*PartitionMetadataResponse, error)
```

Liftbridge provides a partition metadata API which can be used to retrieve
information for a partition. Most importantly, there are use cases where it is
useful to retrieve the high watermark and newest offset of the partition for
tighter control over subscriptions and published messages. A few key points to
take into account:

- The client must connect to the partition leader to fetch partition metadata.
  Only the partition leader maintains the most up-to-date version of the high
  watermark and newest offset. Thus, the request must be sent to the partition
  leader.
- The client should maintain an internal version of the metadata to know, for a
  given partition of a stream, which broker is currently the leader. It should
  be noted that the partition leader is different from the cluster's metadata
  leader.

In the Go client example above, `FetchPartitionMetadata` takes three arguments:

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. | language-dependent |
| stream | string | Stream name | yes |
| partition | int | ID of the partition | yes |

[Implementation Guidance](#fetchpartitionmetadata-implementation)

### SetCursor

```go
// SetCursor persists a cursor position for a particular stream partition.
// This can be used to checkpoint a consumer's position in a stream to
// resume processing later.
SetCursor(ctx context.Context, id, stream string, partition int32, offset int64) error
```

`SetCursor` is used to checkpoint a consumer's position in a stream partition.
This is useful to allow consumer's to resume processing a stream while
remaining stateless by using [`FetchCursor`](#fetchcursor).

Because cursors are stored in an internal stream that is partitioned, clients
need to send this request to the appropriate partition leader. The internal
cursors stream is partitioned by cursor key. A cursor key consists of
`<cursorID>,<stream>,<partition>`. To map this key to a partition, hash it with
an IEEE CRC32 and then mod by the number of partitions in the `__cursors`
stream. This request mapping should be handled by the client implementation.

`SetCursor` throws an error/exception if the cursors stream does not exist, the
particular cursors partition does not exist, the server is not the leader for
the cursors partition, or the server failed to durably store the cursor.

In the Go client example above, `SetCursor` takes five arguments:

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. | language-dependent |
| id | string | Unique cursor identifier | yes |
| stream | string | Name of stream cursor belongs to | yes |
| partition | int | ID of the stream partition cursor belongs to | yes |
| offset | int | Cursor offset position | yes |

[Implementation Guidance](#setcursor-implementation)

### FetchCursor

```go
// FetchCursor retrieves a cursor position for a particular stream
// partition. It returns -1 if the cursor does not exist.
FetchCursor(ctx context.Context, id, stream string, partition int32) (int64, error)
```

`FetchCursor` is used to retrieve a consumer's position in a stream partition
stored by [`SetCursor`](#setcursor).  This is useful to allow consumer's to
resume processing a stream while remaining stateless.

Because cursors are stored in an internal stream that is partitioned, clients
need to send this request to the appropriate partition leader. The internal
cursors stream is partitioned by cursor key. A cursor key consists of
`<cursorID>,<stream>,<partition>`. To map this key to a partition, hash it with
an IEEE CRC32 and then mod by the number of partitions in the `__cursors`
stream. This request mapping should be handled by the client implementation.

`FetchCursor` throws an error/exception if the cursors stream does not exist,
the particular cursors partition does not exist, the server is not the leader
for the cursors partition, or the server failed to load the cursor.

In the Go client example above, `FetchCursor` takes four arguments:

| Argument | Type | Description | Required |
|:----|:----|:----|:----|
| context | context | A [context](https://golang.org/pkg/context/#Context) which is a Go idiom for passing things like a timeout, cancellation signal, and other values across API boundaries. For Liftbridge, this is primarily used for two things: request timeouts and cancellation. In other languages, this might be replaced by explicit arguments, optional named arguments, or other language-specific idioms. | language-dependent |
| id | string | Unique cursor identifier | yes |
| stream | string | Name of stream cursor belongs to | yes |
| partition | int | ID of the stream partition cursor belongs to | yes |

[Implementation Guidance](#fetchcursor-implementation)

### Close

```go
// Close the client connection.
func Close() error
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

### Messages and Acks

It's recommended to not expose transport and protocol details to users in
client library implementations. Currently, Liftbridge relies on gRPC and
Protocol Buffers, but it's possible that this could change in the future. These
should be considered implementation details. Thus, it's recommended to provide
wrapping types around `Message`, `Ack`, `AckPolicy`, and any other API objects.
Implementations should handle converting protobufs into these wrappers. The Go
implementation of these is shown below to demonstrate:

```go
// Message received from a Liftbridge stream.
type Message struct {
	offset       int64
	key          []byte
	value        []byte
	partition    int32
	timestamp    time.Time
	stream       string
	subject      string
	replySubject string
	headers      map[string][]byte
}

func messageFromProto(wireMsg *proto.Message) *Message {
	if wireMsg == nil {
		return nil
	}
	msg := &Message{
		offset:       wireMsg.GetOffset(),
		key:          wireMsg.GetKey(),
		value:        wireMsg.GetValue(),
		partition:    wireMsg.GetPartition(),
		timestamp:    time.Unix(0, wireMsg.GetTimestamp()),
		stream:       wireMsg.GetStream(),
		subject:      wireMsg.GetSubject(),
		replySubject: wireMsg.GetReplySubject(),
		headers:      wireMsg.GetHeaders(),
	}
	return msg
}

// Offset is a monotonic message sequence in the stream partition.
func (m *Message) Offset() int64 {
	return m.offset
}

// Key is an optional label set on a Message, useful for partitioning and
// stream compaction.
func (m *Message) Key() []byte {
	return m.key
}

// Value is the Message payload.
func (m *Message) Value() []byte {
	return m.value
}

// Timestamp is the time the Message was received by the server.
func (m *Message) Timestamp() time.Time {
	return m.timestamp
}

// Subject is the NATS subject the Message was received on.
func (m *Message) Subject() string {
	return m.subject
}

// ReplySubject is the NATS reply subject on the Message, if any.
func (m *Message) ReplySubject() string {
	return m.replySubject
}

// Headers is a set of key-value pairs.
func (m *Message) Headers() map[string][]byte {
	headers := make(map[string][]byte, len(m.headers))
	for key, value := range m.headers {
		headers[key] = value
	}
	return headers
}

// Stream the Message was received on.
func (m *Message) Stream() string {
	return m.stream
}

// Partition the Message was received on.
func (m *Message) Partition() int32 {
	return m.partition
}

// AckPolicy controls the behavior of message acknowledgements.
type AckPolicy int32

func (a AckPolicy) toProto() proto.AckPolicy {
	return proto.AckPolicy(a)
}

// Ack represents an acknowledgement that a message was committed to a stream
// partition.
type Ack struct {
	stream             string
	partitionSubject   string
	messageSubject     string
	offset             int64
	ackInbox           string
	correlationID      string
	ackPolicy          AckPolicy
	receptionTimestamp time.Time
	commitTimestamp    time.Time
}

func ackFromProto(wireAck *proto.Ack) *Ack {
	if wireAck == nil {
		return nil
	}
	ack := &Ack{
		stream:             wireAck.GetStream(),
		partitionSubject:   wireAck.GetPartitionSubject(),
		messageSubject:     wireAck.GetMsgSubject(),
		offset:             wireAck.GetOffset(),
		ackInbox:           wireAck.GetAckInbox(),
		correlationID:      wireAck.GetCorrelationId(),
		ackPolicy:          AckPolicy(wireAck.GetAckPolicy()),
		receptionTimestamp: time.Unix(0, wireAck.GetReceptionTimestamp()),
		commitTimestamp:    time.Unix(0, wireAck.GetCommitTimestamp()),
	}
	return ack
}

// Stream the Message was received on.
func (a *Ack) Stream() string {
	return a.stream
}

// PartitionSubject is the NATS subject the partition is attached to.
func (a *Ack) PartitionSubject() string {
	return a.partitionSubject
}

// MessageSubject is the NATS subject the message was received on.
func (a *Ack) MessageSubject() string {
	return a.messageSubject
}

// Offset is the partition offset the message was committed to.
func (a *Ack) Offset() int64 {
	return a.offset
}

// AckInbox is the NATS subject the ack was published to.
func (a *Ack) AckInbox() string {
	return a.ackInbox
}

// CorrelationID is the user-supplied value from the message.
func (a *Ack) CorrelationID() string {
	return a.correlationID
}

// AckPolicy sent on the message.
func (a *Ack) AckPolicy() AckPolicy {
	return a.ackPolicy
}

// ReceptionTimestamp is the timestamp the message was received by the server.
func (a *Ack) ReceptionTimestamp() time.Time {
	return a.receptionTimestamp
}

// CommitTimestamp is the timestamp the message was committed.
func (a *Ack) CommitTimestamp() time.Time {
	return a.commitTimestamp
}
```

With [`PublishAsync`](#publishasync), the server can also send back `PublishResponse` messages
containing a `PublishAsyncError` in the event of a failed publish. Similar to
`Ack`, client implementations should wrap this error object before exposing it
to the user. An example of this is shown in the Go implementation
`asyncErrorFromProto` below.

```go
func asyncErrorFromProto(asyncError *proto.PublishAsyncError) error {
	if asyncError == nil {
		return nil
	}
	switch asyncError.Code {
	case proto.PublishAsyncError_NOT_FOUND:
		return ErrNoSuchPartition
	case proto.PublishAsyncError_READONLY:
		return ErrReadonlyPartition
	default:
		return errors.New(asyncError.Message)
	}
}
```

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
- `DeleteStream`
- `PauseStream`
- `SetStreamReadonly`
- `PublishToSubject`
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

Additionally, RPCs such as `SetCursor`, `FetchCursor`, and
`FetchPartitionMetadata` cannot be sent to any random server but, rather, need
to be sent to leaders of particular partitions. Thus, it's also useful to
implement a variant of `doResilientRPC` for sending RPCs to partition leaders
in a fault-tolerant fashion. The Go implementation of this is called
`doResilientLeaderRPC` and is shown below.

```go
// doResilientLeaderRPC sends the given RPC to the partition leader and
// performs retries if it fails due to the broker being unavailable.
func (c *client) doResilientLeaderRPC(ctx context.Context, rpc func(client proto.APIClient) error,
	stream string, partition int32) (err error) {

	var (
		pool *connPool
		addr string
		conn *conn
	)
	for i := 0; i < 5; i++ {
		pool, addr, err = c.getPoolAndAddr(stream, partition, false)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			c.metadata.update(ctx)
			continue
		}
		conn, err = pool.get(c.connFactory(addr))
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			c.metadata.update(ctx)
			continue
		}
		err = rpc(conn)
		pool.put(conn)
		if err != nil {
			if status.Code(err) == codes.Unavailable {
				time.Sleep(50 * time.Millisecond)
				c.metadata.update(ctx)
				continue
			}
		}
		break
	}
	return err
}
```

`getPoolAndAddr` is a helper which returns a connection pool and the address
for the leader of the given partition.

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

Also, clients can set custom configurations for the stream to be created. The
exhaustive list of supported stream configurations are:

```plaintext
RetentionMaxBytes
RetentionMaxMessages
RetentionMaxAge
CleanerInterval
SegmentMaxBytes
SegmentMaxAge
CompactEnabled
CompactMaxGoroutines
OptimisticConcurrencyControl
Encryption
```

Refer to [Stream Configuration](configuration.md#streams-configuration-settings)
for more details. Note that these settings are optional. If not provided, the
default configurations of the broker will be used instead. 

In order to differentiate between custom configuration specified by the user
and the server's default configuration, we use 3 custom `NullableType` wrappers
in setting options for the `CreateStreamRequest`. These custom types are:

```proto
message NullableInt64 {
    int64 value = 1; 
}

message NullableInt32 {
    int32 value = 1; 
}

message NullableBool {
    bool value = 1; 
}

```

Note: if `CompactMaxGoroutines` is configured, you have to make sure manually
that `CompactEnabled` is also set. The reason is that if this is not enabled
explicitly, the servier will use the default configuration and that may be to
disable compaction on the service side, which renders `CompactMaxGoroutines` to
be unused.

Note: if `OptimisticConcurrencyControl` is configured, you should to make sure
that at least one `AckPolicy` is set. A `PublishRequest` with `AckPolicy` is set to `AckPolicy_NONE`
will be rejected explicity by the server.

Note: if `Encryption` is configured, you should make sure that an environment variable `LIFTBRIDGE_ENCRYPTION_KEY` is set. This is the master key that will be used to wrap data key. See [Encryption of Data-at-rest](concepts.md#Encryption-of-data-at-rest-on-server-side). The value should be a valid 128-bit or 256-bit AES key.

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

	req := opts.newRequest(subject, name)
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

`StreamOptions.newRequest` creates a `CreateStreamRequest` protobuf and applies
the specified options to it.

### DeleteStream Implementation

The `DeleteStream` implementation simply constructs a gRPC request and executes
it using the [resilient RPC method](#rpcs) described above. If the
`NotFound` gRPC error is returned, an `ErrNoSuchStream` error/exception is
thrown. Otherwise, any other error/exception is thrown if the operation failed.

```go
// DeleteStream deletes a stream and all of its partitions. Name is the stream
// identifier, globally unique.
func (c *client) DeleteStream(ctx context.Context, name string) error {
	req := &proto.DeleteStreamRequest{Name: name}
	err := c.doResilientRPC(func(client proto.APIClient) error {
		_, err := client.DeleteStream(ctx, req)
		return err
	})
	if status.Code(err) == codes.NotFound {
		return ErrNoSuchStream
	}
	return err
}
```

### PauseStream Implementation

The `PauseStream` implementation simply constructs a gRPC request and executes
it using the [resilient RPC method](#rpcs) described above. If the `NotFound`
gRPC error is returned, an `ErrNoSuchPartition` error/exception is thrown.
Otherwise, any other error/exception is thrown if the operation failed.

```go
// PauseStream pauses a stream and some or all of its partitions. Name is the
// stream identifier, globally unique. It returns an ErrNoSuchPartition if the
// given stream or partition does not exist. By default, this will pause all
// partitions. A partition is resumed when it is published to via the
// Liftbridge Publish API or ResumeAll is enabled and another partition in the
// stream is published to.
func (c *client) PauseStream(ctx context.Context, name string, options ...PauseOption) error {
	opts := &PauseOptions{}
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return err
		}
	}

	req := &proto.PauseStreamRequest{
		Name:       name,
		Partitions: opts.Partitions,
		ResumeAll:  opts.ResumeAll,
	}
	err := c.doResilientRPC(func(client proto.APIClient) error {
		_, err := client.PauseStream(ctx, req)
		return err
	})
	if status.Code(err) == codes.NotFound {
		return ErrNoSuchPartition
	}
	return err
}
```

### SetStreamReadonly Implementation

The `SetStreamReadonly` implementation simply constructs a gRPC request and
executes it using the [resilient RPC method](#rpcs) described above. If the
`NotFound` gRPC error is returned, an `ErrNoSuchPartition` error/exception is
thrown.  Otherwise, any other error/exception is thrown if the operation
failed.

```go
// SetStreamReadonly sets the readonly flag on a stream and some or all of
// its partitions. Name is the stream identifier, globally unique. It
// returns an ErrNoSuchPartition if the given stream or partition does not
// exist. By default, this will set the readonly flag on all partitions.
// Subscribers to a readonly partition will see their subscription ended
// with a ErrReadonlyPartition error once all messages currently in the
// partition have been read.
func (c *client) SetStreamReadonly(ctx context.Context, name string, options ...ReadonlyOption) error {
	opts := &ReadonlyOptions{}
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return err
		}
	}

	req := &proto.SetStreamReadonlyRequest{
		Name:       name,
		Partitions: opts.Partitions,
		Readonly:   !opts.Readwrite,
	}
	err := c.doResilientRPC(func(client proto.APIClient) error {
		_, err := client.SetStreamReadonly(ctx, req)
		return err
	})
	if status.Code(err) == codes.NotFound {
		return ErrNoSuchPartition
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

The `Subscribe` RPC can be sent to either the leader or a random ISR replica of
the partition the client wants to subscribe to. By default, it should subscribe
to the leader. A `ReadISRReplica` option must be enabled on the subscribe
request in order to subscribe to an ISR replica.

Whether subscribing to the leader (by default) or a random ISR replica, the
address should be determined by [fetching the metadata](#fetchmetadata) from
the cluster. This metadata can (and should) be cached in the client. It's
recommended `Subscribe` use [connection pooling](#connection-pooling) since
each `Subscribe` call will involve a long-lived gRPC connection to a different
server.

When the subscription stream is created, the server sends an empty message to
indicate the subscription was successfully created. Otherwise, an error is sent
on the stream if the subscribe failed. This handshake message must be handled
and should not be exposed to the user. A gRPC `FailedPrecondition` error
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

`Publish` originally used the `Publish` RPC endpoint. This endpoint will be
deprecated in a future version of Liftbridge. Instead, `Publish` should rely on
the `PublishAsync` streaming endpoint and wrap it with synchronous logic. Refer
to the [`PublishAsync` implementation](#publishasync-implementation) for
guidance on implementing the asynchronous component. The remainder of this
builds on that implementation.

`Publish` relies on a private `publishAsync` helper function by synchronously
waiting for the dispatched ack. If the ack policy is `NONE`, the message is
published while ignoring the ack. Otherwise, `Publish` waits for the ack or
error dispatched by `publishAsync`. The Go implementation of this is shown
below. Refer to the [`PublishAsync` implementation](#publishasync-implementation)
for the implementation of `publishAsync`.

```go
// Publish publishes a new message to the Liftbridge stream. The partition that
// gets published to is determined by the provided partition or Partitioner
// passed through MessageOptions, if any. If a partition or Partitioner is not
// provided, this defaults to the base partition. This partition determines the
// underlying NATS subject that gets published to.  To publish directly to a
// spedcific NATS subject, use the low-level PublishToSubject API.
//
// If the AckPolicy is not NONE, this will synchronously block until the ack is
// received. If the ack is not received in time, ErrAckTimeout is returned. If
// AckPolicy is NONE, this returns nil on success.
func (c *client) Publish(ctx context.Context, stream string, value []byte,
	options ...MessageOption) (*Ack, error) {

	opts := &MessageOptions{Headers: make(map[string][]byte)}
	for _, opt := range options {
		opt(opts)
	}

	if opts.AckPolicy == AckPolicy(proto.AckPolicy_NONE) {
		// Fire and forget.
		err := c.publishAsync(ctx, stream, value, nil, opts)
		return nil, err
	}

	// Publish and wait for ack.
	var (
		ackCh   = make(chan *Ack, 1)
		errorCh = make(chan error, 1)
	)
	err := c.publishAsync(ctx, stream, value, func(ack *Ack, err error) {
		if err != nil {
			errorCh <- err
			return
		}
		ackCh <- ack
	}, opts)
	if err != nil {
		return nil, err
	}

	select {
	case ack := <-ackCh:
		return ack, nil
	case err := <-errorCh:
		return nil, err
	}
}
```

### PublishAsync Implementation

The `PublishAsync` RPC endpoint is a bidirectional streaming API. This means
both the client can continually send new messages to the server and the server
can continually send acks back to the client. This requires the client to have
a background connection to a server in the cluster for handling asynchronous
publishes. This also requires a long-running process for dispatching acks
received from the server. We recommend initializing a dedicated gRPC connection
for `PublishAsync` and starting a background thread for dispatching acks when
the client is initialized. In the Go implementation, we start a `dispatchAcks`
goroutine, shown below, which receives acks from the server and dispatches the
appropriate callback. `PublishResponse`, the message sent by the server, also
includes a `PublishAsyncError`. This is set if an async publish failed, e.g.
because the partition published to does not exist.

Since the `PublishAsync` RPC is a long-lived streaming endpoint, it's possible
for the connection to be disrupted, such as in the case of a server failure.
Therefore, it's important that this connection be re-established in the event
of a disconnection. Note that there are two different threads of execution
which use the `PublishAsync` connection concurrently, one for publishing
messages and one for receiving acks. To simplify the reconnect logic, we
recommend handling the reconnect only on the ack dispatch side. This also has
the added benefit of avoiding duplicate deliveries in the event of a disconnect
during publish. Thus, it's up to end users to implement retries around publish
in order to handle these types of transient failures. The Go implementation of
`dispatchAcks` along with a helper function for re-establishing the
`PublishAsync` connection called `newAsyncStream` is shown below.

```go
func (c *client) dispatchAcks() {
	c.mu.RLock()
	asyncStream := c.asyncStream
	c.mu.RUnlock()
	for {
		resp, err := asyncStream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			stream, ok := c.newAsyncStream()
			if !ok {
				return
			}
			asyncStream = stream
			c.mu.Lock()
			c.asyncStream = stream
			c.mu.Unlock()
			continue
		}

		var correlationID string
		if resp.AsyncError != nil {
			correlationID = resp.CorrelationId
		} else if resp.Ack != nil {
			// TODO: Use resp.CorrelationId once Ack.CorrelationId is removed.
			correlationID = resp.Ack.CorrelationId
		}
		ctx := c.removeAckContext(correlationID)
		if ctx != nil && ctx.handler != nil {
			ctx.handler(ackFromProto(resp.Ack), asyncErrorFromProto(resp.AsyncError))
		}
	}
}

func (c *client) newAsyncStream() (stream proto.API_PublishAsyncClient, ok bool) {
	for {
		err := c.doResilientRPC(func(client proto.APIClient) error {
			resp, err := client.PublishAsync(context.Background())
			if err != nil {
				return err
			}
			stream = resp
			return nil
		})
		if err == nil {
			return stream, true
		}
		if c.isClosed() {
			return nil, false
		}
		time.Sleep(50 * time.Millisecond)
	}
}
```

Recall that `dispatchAcks` is invoked as a long-running goroutine on client
initialization. The client also maintains a mapping of correlation ID to
`ackContext`, which is a struct used to store a timer and ack handler for a
given message. When an ack is received from the server, its `ackContext` is
removed from the map and the handler callback is invoked. The timer is used to
implement an ack timeout, which similarly invokes the handler callback but with
an error instead of the ack.

```go
// ackContext tracks state for an in-flight message expecting an ack.
type ackContext struct {
	handler AckHandler
	timer   *time.Timer
}

func (c *client) removeAckContext(cid string) *ackContext {
	var timer *time.Timer
	c.mu.Lock()
	ctx := c.ackContexts[cid]
	if ctx != nil {
		timer = ctx.timer
		delete(c.ackContexts, cid)
	}
	c.mu.Unlock()
	// Cancel ack timeout if any.
	if timer != nil {
		timer.Stop()
	}
	return ctx
}
```

So far, we have covered how the `PublishAsync` streaming connection is managed
and how acks are dispatched. Now we will look at the actual publish
implementation. Both [`Publish`](#publish-implementation) and `PublishAsync`
rely on a private `publishAsync` helper function.

```go
// PublishAsync publishes a new message to the Liftbridge stream and
// asynchronously processes the ack or error for the message.
func (c *client) PublishAsync(ctx context.Context, stream string, value []byte,
	ackHandler AckHandler, options ...MessageOption) error {

	opts := &MessageOptions{Headers: make(map[string][]byte)}
	for _, opt := range options {
		opt(opts)
	}
	return c.publishAsync(ctx, stream, value, ackHandler, opts)
}
```

This `publishAsync` helper function handles assigning a correlation ID to the
message options (if one is not specified) and constructs a `PublishRequest` for
sending to the server. It then sets up the `ackContext` and sends the
`PublishRequest` to the server using the `PublishAsync` gRPC stream. There is a
client option for configuring a global ack timeout for publishes called
`AckWaitTime`. This can be overridden on individual publishes. In the Go
implementation, shown below, this is done via the `Context` deadline.

```go
func (c *client) publishAsync(ctx context.Context, stream string, value []byte,
	ackHandler AckHandler, opts *MessageOptions) error {

	if opts.CorrelationID == "" {
		opts.CorrelationID = nuid.Next()
	}

	req, err := c.newPublishRequest(ctx, stream, value, opts)
	if err != nil {
		return err
	}

	c.mu.Lock()
	asyncStream := c.asyncStream
	if ackHandler != nil {
		// Setup ack timeout.
		var (
			timeout      = c.opts.AckWaitTime
			deadline, ok = ctx.Deadline()
		)
		if ok {
			timeout = time.Until(deadline)
		}
		ack := &ackContext{
			handler: ackHandler,
			timer: time.AfterFunc(timeout, func() {
				ackCtx := c.removeAckContext(req.CorrelationId)
				// Ack was processed before timeout finished.
				if ackCtx == nil {
					return
				}
				if ackCtx.handler != nil {
					ackCtx.handler(nil, ErrAckTimeout)
				}
			}),
		}
		c.ackContexts[req.CorrelationId] = ack
	}
	c.mu.Unlock()

	if err := asyncStream.Send(req); err != nil {
		c.removeAckContext(req.CorrelationId)
		return err
	}

	return nil
}
```

`newPublishRequest` determines which stream partition to publish to using the
`partition` helper and then constructs the `PublishRequest`.

```go
func (c *client) newPublishRequest(ctx context.Context, stream string, value []byte,
	opts *MessageOptions) (*proto.PublishRequest, error) {

	// Determine which partition to publish to.
	partition, err := c.partition(ctx, stream, opts.Key, value, opts)
	if err != nil {
		return nil, err
	}

	return &proto.PublishRequest{
		Stream:        stream,
		Partition:     partition,
		Key:           opts.Key,
		Value:         value,
		AckInbox:      opts.AckInbox,
		CorrelationId: opts.CorrelationID,
		AckPolicy:     opts.AckPolicy.toProto(),
	}, nil
}

// partition determines the partition ID to publish the message to. If a
// partition was explicitly provided, it will be returned. If a Partitioner was
// provided, it will be used to compute the partition. Otherwise, 0 will be
// returned.
func (c *client) partition(ctx context.Context, stream string, key, value []byte,
	opts *MessageOptions) (int32, error) {

	var partition int32
	// If a partition is explicitly provided, use it.
	if opts.Partition != nil {
		partition = *opts.Partition
	} else if opts.Partitioner != nil {
		// Make sure we have metadata for the stream and, if not, update it.
		metadata, err := c.waitForStreamMetadata(ctx, stream)
		if err != nil {
			return 0, err
		}
		partition = opts.Partitioner.Partition(stream, key, value, metadata)
	}
	return partition, nil
}
```

The partition to publish to is determined in the following way:

1. If a partition is explicitly provided using the `ToPartition` message
   option, use it.
2. If a `Partitioner` is provided, use it to compute the partition.
3. If neither of the above apply, use partition 0 (the default partition).

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
// key and modding by the number of stream partitions.
func (k *keyPartitioner) Partition(stream string, key, value []byte, metadata *Metadata) int32 {
	if key == nil {
		key = []byte("")
	}

	partitions := metadata.PartitionCountForStream(stream)
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
// fashion by atomically incrementing a counter for the message stream and
// modding by the number of stream partitions.
func (r *roundRobinPartitioner) Partition(stream string, key, value []byte, metadata *Metadata) int32 {
	partitions := metadata.PartitionCountForStream(stream)
	if partitions == 0 {
		return 0
	}
	r.Lock()
	counter, ok := r.streamCounterMap[stream]
	if !ok {
		counter = new(streamCounter)
		r.streamCounterMap[stream] = counter
	}
	r.Unlock()
	counter.Lock()
	count := counter.count
	counter.count++
	counter.Unlock()
	return count % partitions
}
```

### PublishToSubject Implementation

`PublishToSubject` sends a `PublishToSubjectRequest` to the `PublishToSubject`
endpoint using the using the [resilient RPC method](#rpcs) described above.
Unlike [`Publish`](#publish-implementation) and [`PublishAsync`](#publishasync-implementation),
this sets the publish subject directly rather than computing a partition for a
stream. The Go implementation of this is shown below:

```go
// PublishToSubject publishes a new message to the NATS subject. Note that
// because this publishes directly to a subject, there may be multiple (or no)
// streams that receive the message. As a result, MessageOptions related to
// partitioning will be ignored. To publish at the stream/partition level, use
// the high-level Publish API.
//
// If the AckPolicy is not NONE and a deadline is provided, this will
// synchronously block until the first ack is received. If an ack is not
// received in time, ErrAckTimeout is returned. If an AckPolicy and deadline
// are configured, this returns the first Ack on success, otherwise it returns
// nil.
func (c *client) PublishToSubject(ctx context.Context, subject string, value []byte,
	options ...MessageOption) (*Ack, error) {

	opts := &MessageOptions{Headers: make(map[string][]byte)}
	for _, opt := range options {
		opt(opts)
	}

	req := &proto.PublishToSubjectRequest{
		Subject:       subject,
		Key:           opts.Key,
		Value:         value,
		AckInbox:      opts.AckInbox,
		CorrelationId: opts.CorrelationID,
		AckPolicy:     opts.AckPolicy.toProto(),
	}

	// Setup ack timeout.
	var (
		cancel func()
		_, ok  = ctx.Deadline()
	)
	if !ok {
		ctx, cancel = context.WithTimeout(ctx, c.opts.AckWaitTime)
		defer cancel()
	}

	var ack *proto.Ack
	err := c.doResilientRPC(func(client proto.APIClient) error {
		resp, err := client.PublishToSubject(ctx, req)
		if err == nil {
			ack = resp.Ack
		}
		return err
	})
	if status.Code(err) == codes.DeadlineExceeded {
		err = ErrAckTimeout
	}
	return ackFromProto(ack), err
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

// PartitionCountForStream returns the number of partitions for the given
// stream.
func (m *Metadata) PartitionCountForStream(stream string) int32 {}
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

### FetchPartitionMetadata Implementation

`FetchPartitionMetadata` should return an immutable object which exposes
partition metadata. 

*NOTE*: *It is important to keep in mind that the
`FetchPartitionMetadataRequest` should be sent only to the partition leader. It
is the job of the client to figure out which broker is currently the partition
leader. `Metadata` can be used to figure out which broker is currently the
leader of the requested partition*. In Go, this is implemented using a variant
of the [resilient RPC method](#rpcs) described above.

The object contains information of the partition, notably the high watermark
and newest offset, which is necessary in case the client wants tighter control
over the subscription/publication of messages.

### SetCursor Implementation

The `SetCursor` implementation simply constructs a gRPC request and
executes it using the [resilient leader RPC method](#rpcs) described above.
This requires mapping the cursor to a partition in the internal `__cursors`
stream. A cursor is mapped to a partition using its key. The cursor key
consists of `<cursorID>.<stream>.<partition>`. This key is then hashed using
IEEE CRC32 and modded by the number of partitions in the `__cursors` stream.
The Go implementation of this is shown below.

```go
// SetCursor persists a cursor position for a particular stream partition.
// This can be used to checkpoint a consumer's position in a stream to resume
// processing later.
func (c *client) SetCursor(ctx context.Context, id, stream string, partition int32, offset int64) error {
	req := &proto.SetCursorRequest{
		Stream:    stream,
		Partition: partition,
		CursorId:  id,
		Offset:    offset,
	}
	cursorsPartition, err := c.getCursorsPartition(ctx, c.getCursorKey(id, stream, partition))
	if err != nil {
		return err
	}
	return c.doResilientLeaderRPC(ctx, func(client proto.APIClient) error {
		_, err := client.SetCursor(ctx, req)
		return err
	}, cursorsStream, cursorsPartition)
}

func (c *client) getCursorKey(cursorID, streamName string, partitionID int32) []byte {
	return []byte(fmt.Sprintf("%s,%s,%d", cursorID, streamName, partitionID))
}

func (c *client) getCursorsPartition(ctx context.Context, cursorKey []byte) (int32, error) {
	// Make sure we have metadata for the cursors stream and, if not, update it.
	metadata, err := c.waitForStreamMetadata(ctx, cursorsStream)
	if err != nil {
		return 0, err
	}

	stream := metadata.GetStream(cursorsStream)
	if stream == nil {
		return 0, errors.New("cursors stream does not exist")
	}

	return int32(hasher(cursorKey) % uint32(len(stream.Partitions()))), nil
}
```

### FetchCursor Implementation

The `FetchCursor` implementation simply constructs a gRPC request and
executes it using the [resilient leader RPC method](#rpcs) described above.
This requires mapping the cursor to a partition in the internal `__cursors`
stream. A cursor is mapped to a partition using its key. The cursor key
consists of `<cursorID>.<stream>.<partition>`. This key is then hashed using
IEEE CRC32 and modded by the number of partitions in the `__cursors` stream.
The Go implementation of this is shown below.

```go
// FetchCursor retrieves a cursor position for a particular stream partition.
// It returns -1 if the cursor does not exist.
func (c *client) FetchCursor(ctx context.Context, id, stream string, partition int32) (int64, error) {
	var (
		req = &proto.FetchCursorRequest{
			Stream:    stream,
			Partition: partition,
			CursorId:  id,
		}
		offset int64
	)
	cursorsPartition, err := c.getCursorsPartition(ctx, c.getCursorKey(id, stream, partition))
	if err != nil {
		return 0, err
	}
	err = c.doResilientLeaderRPC(ctx, func(client proto.APIClient) error {
		resp, err := client.FetchCursor(ctx, req)
		if err != nil {
			return err
		}
		offset = resp.Offset
		return nil
	}, cursorsStream, cursorsPartition)
	return offset, err
}
```

The `getCursorKey` and `getCursorsPartition` helper implementations are shown
[above](#setcursor-implementation).

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
