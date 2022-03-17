---
id: version-v1.8.0-faq
title: Frequently Asked Questions
original_id: faq
---

## What is Liftbridge?

Liftbridge is a server that implements a durable, replicated, and scalable
message log. Clients create streams which are partitioned for horizontal
scalability and replicated for high availability. Streams record messages to a
durable write-ahead log.

Liftbridge is implemented on top of [NATS](https://nats.io), a lightweight,
high-performance pub/sub messaging system. This means it can be added to an
existing NATS deployment to provide message durability with no code changes. If
you are not already using NATS or not familiar with it, Liftbridge can be
deployed with NATS as an implementation detail.

## Why was it created?

The vision for Liftbridge is to provide a "Kafka-lite" solution
designed with the [Go](https://go.dev) community first in mind. Unlike Kafka,
which is built on the JVM and whose canonical client library is Java (or the
C-based librdkafka), Liftbridge and its canonical client,
[go-liftbridge](https://github.com/liftbridge-io/go-liftbridge), are
implemented in Go. The ultimate goal of Liftbridge is to provide a lightweight
message-streaming solution with a focus on simplicity and usability.

Liftbridge was designed to bridge the gap between sophisticated but complex
log-based messaging systems like Apache Kafka and Apache Pulsar and simpler,
cloud-native solutions. There is no ZooKeeper or other unwieldy dependencies,
no JVM, no complicated API or configuration, and client libraries are
implemented using [gRPC](https://grpc.io/). 

## How does it scale?

Liftbridge has several mechanisms for horizontal scaling of message consumption.
Brokers can be added to the cluster and additional streams can be created which
are then distributed among the cluster. In effect, this splits out message
routing from storage and consumption, which allows Liftbridge to scale
independently and eschew subject partitioning.

Streams can also join a load-balance group, which effectively load balances a
NATS subject among the streams in the group without affecting delivery to
other streams.

Additionally, streams can be partitioned, allowing messages to be divided up
among the brokers in a cluster. In fact, all streams are partitioned with the
default case being a single partition.

[Consumer groups](./consumer_groups.md) allow for load balancing of stream
consumption. In combination with stream partitioning, this allows for increased
parallelism and higher throughput.

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
