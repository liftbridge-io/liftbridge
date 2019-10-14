---
id: faq
title: Frequently Asked Questions
---

## What is Liftbridge?

Liftbridge is a server that implements a durable, replicated message log for
[NATS](https://github.com/nats-io/gnatsd). Clients create a named *stream*
which is attached to a NATS subject. The stream then records messages on that
subject to a replicated write-ahead log. Multiple consumers can read back
from the same stream, and multiple streams can be attached to the same
subject. Liftbridge provides a Kafka-like API in front of NATS. See the
Liftbridge [overview](overview.md) for more information.

## Why was it created?

Liftbridge was designed to bridge the gap between sophisticated log-based
messaging systems like Apacha Kafka and Apache Pulsar and simpler, cloud-native
systems. There is no ZooKeeper or other unwieldy dependencies, no JVM, no
complicated API, and client libraries are just [gRPC](https://grpc.io/). More
importantly, Liftbridge aims to extend NATS with a durable, at-least-once
delivery mechanism that upholds the NATS tenets of simplicity, performance, and
scalability. Unlike [NATS
Streaming](https://github.com/nats-io/nats-streaming-server), it uses the core
NATS protocol with optional extensions. This means it can be added to an
existing NATS deployment to provide message durability with no code changes.

## Why not NATS Streaming?

[NATS Streaming](https://github.com/nats-io/nats-streaming-server) provides a
similar log-based messaging solution. However, it is an entirely separate
protocol built on top of NATS. NATS is simply the transport for NATS Streaming.
This means there is no "cross-talk" between messages published to NATS and
messages published to NATS Streaming.

Liftbridge was built to *augment* NATS with durability rather than providing a
completely separate system. NATS Streaming also provides a broader set of
features such as durable subscriptions, queue groups, pluggable storage
backends, and multiple fault-tolerance modes. Liftbridge aims to have a small
API surface area.

The key features that differentiate Liftbridge are the shared message namespace,
wildcards, log compaction, and horizontal scalability. NATS Streaming replicates
channels to the entire cluster through a single Raft group. Liftbridge allows
replicating to a subset of the cluster, and each stream is replicated
independently. This allows the cluster to scale horizontally. NATS Streaming
also does not support channel partitioning, requiring it to be implemented at
the application layer. Liftbridge has built-in support for stream partitioning.

## How does it scale?

Liftbridge has several mechanisms for horizontal scaling of message consumption.
Brokers can be added to the cluster and additional streams can be created which
are then distributed among the cluster. In effect, this splits out message
routing from storage and consumption, which allows Liftbridge to scale
independently and eschew subject partitioning.

Streams can also join a load-balance group, which effectively load balances a
NATS subject among the streams in the group without affecting delivery to
other streams.

Finally, streams can be partitioned, allowing messages to be divided up among
the brokers in a cluster. In fact, all streams are partitioned with the default
case being a single partition.

## What about HA?

High availability is achieved by replicating the streams. When a stream is
created, the client specifies a `replicationFactor`, which determines the
number of brokers to replicate the stream's partitions. Each stream partition
has a leader who is responsible for handling reads and writes. Followers then
replicate the log from the leader. If the leader fails, one of the followers
will step up to replace it. The replication protocol closely resembles that of
Kafka, so there is much more nuance to avoid data consistency problems. See the
[replication protocol documentation](replication_protocol.md)
for more details.

## What about performance?

*Benchmarks soon to come...*

## Is it production-ready?

No, this project is early and still evolving.
