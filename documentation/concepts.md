---
id: concepts
title: Concepts
---

Liftbridge is a durable stream augmentation for NATS, so it's important to
have a good grasp of the [key concepts in
NATS](https://nats-io.github.io/docs/developer/concepts/intro.html). NATS is a
pub/sub messaging system that centers around the concept of *subjects*. Clients
publish messages to subjects and receive messages from *subscriptions* to
subjects.

## Streams and Partitions

Fundamentally, Liftbridge is just a consumer of NATS subjects. It receives
messages received from NATS subjects and records them in a durable log which
is then exposed to subscribers. Specifically, Liftbridge centers around the
concept of a *stream*, which is a durable message stream attached to a NATS
subject. A stream consists of one or more *partitions*, which are ordered,
replicated, and durably stored on disk and serve as the unit of storage and
parallelism in Liftbridge.

Streams have a few key properties: a subject, which is the corresponding NATS
subject, a name, which is a human-readable identifier for the stream, and a
replication factor, which is the number of nodes the stream's partitions should
be replicated to for redundancy. Optionally, there is a group which is the name
of a load-balance group for the stream to join. When there are multiple streams
in the same group, messages will be balanced among them.

There can be multiple streams attached to the same NATS subject, but stream
names must be unique within a cluster.

By default, streams have a single partition. This partition maps directly to
the stream's NATS subject. If a stream has multiple partitions, each one maps
to a different NATS subject derived from the stream subject. For example, if a
stream with three partitions is attached to the subject "foo", the partitions
will map to the subjects "foo", "foo.1", and "foo.2", respectively.

Each partition has its own message log, leader, and set of followers.

### Write-Ahead Log

Each stream partition is backed by a durable write-ahead log. All reads and
writes to the log go through the partition *leader*, which is selected by the
cluster [controller](#controller). The leader sequences each message in the
partition and sends back an acknowledgement to publishers upon committing a
message to the log. A message is committed to the log once it has been
replicated to the partition's
[in-sync replica set (ISR)](#in-sync-replica-set-isr).

Consumers read committed messages from the log through a subscription on the
partition. They can read back from the log at any arbitrary position, or
*offset*. Additionally, consumers can wait for new messages to be appended
to the log.

### Scalability

Liftbridge is designed to be clustered and horizontally scalable. The
[controller](#controller) is responsible for creating stream partitions.
When a partition is created, the controller selects replicas based on the
stream's replication factor and replicates the partition to the cluster.
Once this replication completes, the partition has been created and the
leader begins processing messages.

As mentioned above, there can exist multiple streams attached to the same NATS
subject or even subjects that are semantically equivalent e.g. "foo.bar" and
"foo.*". Each of these streams will receive a copy of the message as NATS
handles this fan-out.

With this in mind, we can scale linearly by adding more nodes to the Liftbridge
cluster and creating more streams which will be distributed amongst the
cluster members. This has the advantage that we don't need to worry about
partitioning so long as NATS is able to withstand the load. The downside of this
is that it results in redundant processing of messages. Consumers of each stream
are all processing the same set of messages. The other issue is because each
stream is independent of each other, they have separate guarantees. Separate
leaders/followers, separate ISRs, and separate acking means the logs for each
stream are not guaranteed to be identical, even though they are bound to the
same NATS subject.

To accommodate this, streams are partitioned. By default, a stream consists of
just a single partition, but multiple partitions can be created for increased
parallelism. Messages can then be delivered to partitions based on their key,
in a round-robin fashion, randomly, or with some other partitioning strategy
on the client.

Additionally, streams can join a named load-balance group, which load balances
messages on a NATS subject amongst the streams in the group. Load-balance
groups do not affect message delivery to other streams not participating in
the group.

Currently, replicas in Liftbridge act only as a mechanism for high availability
and not scalability. However, there may be work in the future to allow them to
act as read replicas for further scale out.

The diagram below shows a cluster of three servers with a set of streams.
Partitions in yellow indicate the server is the leader for the partition.

![cluster](assets/cluster.png)

### In-Sync Replica Set (ISR)

The In-Sync Replica set (ISR) is a key aspect of the replication protocol in
Liftbridge. The ISR consists of the set of partition replicas that are
currently caught up with the leader. It is equivalent to the [ISR concept in
Kafka](https://kafka.apache.org/documentation/#design_replicatedlog), and the
[replication protocol](./replication_protocol.md) works very similarly.

In order for a message to be committed to a partition's write-ahead log, it
must be acknowledged by all brokers in the ISR. To prevent a single slow
broker from blocking progress, replicas that fall too far behind the leader
are removed from the ISR. The leader does this by making a request to the
controller. In this case, the cluster enters an under-replicated state for
the partition.

Being "too far behind" is controlled by the `replica.max.lag.time`
configuration. This refers to both the maximum amount of time a replica can go
without making a replication request before it's removed and the amount of time
that can pass without being fully caught up with the leader before it's
removed. When a removed replica catches back up with the leader's log, it is
added back into the ISR and the cluster goes back into its fully replicated
state.

Under normal conditions, only a replica from the ISR can be elected the leader
of a partition. This favors data consistency over availability since if the ISR
shrinks too far, there is a risk of being unable to elect a new leader.

### Acknowledgement

Acknowledgements are an opt-in mechanism to guarantee message delivery. If a
[message envelope](#message-envelope) has an `AckInbox`, Liftbridge will send
an ack to this NATS inbox once the message has been committed. This is used to
ensure at-least-once delivery.

Messages can also have an optional `CorrelationId`, which is a user-defined
value which is also set on the server ack to correlate it to a published
message.

There are a couple of things to be aware of with message acknowledgements.
First, if the publisher doesn't care about ensuring its message is stored, it
need not set an `AckInbox`. Second, because there are potentially multiple
(or no) streams attached to a NATS subject (and creation of streams is
dynamic), it's not possible for the publisher to know how many acks to expect.
This is a trade-off we make for enabling subject fan-out and wildcards while
remaining scalable and fast. We make the assertion that if guaranteed delivery
is important, the publisher should be responsible for determining the
destination streams a priori. This allows attaching streams to a subject for
use cases that do not require strong guarantees without the publisher having to
be aware. Note that this might be an area for future improvement to increase
usability. However, this is akin to other similar systems, like Kafka, where
you must first create a topic and then you publish to that topic.

### Subscription

Subscriptions are how Liftbridge streams are consumed. A client subscribes to a
stream partition and specifies a starting offset to begin consuming from. At
this point, the server creates an ephemeral data stream for the client and
begins sending messages to it. Once it consumes up to the end of the log, the
server will wait for more messages to be published until the subscription is
closed by the client.

Subscriptions are not stateful objects. When a subscription is created, there
is no bookkeeping done by the server, aside from the in-memory objects tied to
the lifecycle of the subscription. As a result, the server does not track the
position of a client in the log beyond the scope of a subscription. Stateful
consumer groups will be coming in the near future which will allow a consumer
to pick up where it left off and provide fault-tolerant consumption of
streams.

### Stream Retention and Compaction

Streams support multiple log-retention rules: age-based, message-based, and
size-based. This, for example, allows semantics like "retain messages for 24
hours", "retain 100GB worth of messages", or "retain 1,000,000 messages".

Additionally, Liftbridge supports log *compaction*. Publishers can, optionally,
set a *key* on a [message envelope](#message-envelope). A stream can be
configured to compact by key. In this case, it retains only the last message
for each unique key. Messages that do not have a key are always retained.

## Controller

The controller is the metadata leader for the cluster. Specifically, it is the
*Raft* leader. All operations which require cluster coordination, such as
creating streams, expanding ISRs, shrinking ISRs, or electing stream leaders,
go through the controller and, subsequently, Raft to ensure linearizability.
Raft automatically handles failing over the controller in the event of a
failure for high availability.

Note that in order for the controller to make progress, a quorum (majority) of
the brokers must be running.

Controller is also referred to as "metadata leader" in some contexts.

## Message Envelope

Liftbridge extends NATS by allowing regular NATS messages to flow into durable
streams. This can be completely transparent to publishers. However, it also
allows publishers to *enhance* messages by providing additional metadata and
serializing their messages into *envelopes*. An envelope allows publishers to
set things like the `AckInbox`, `Key`, `Headers`, and other pieces of metadata.
