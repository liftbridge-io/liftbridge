---
id: version-v1.8.0-overview
title: Liftbridge Overview
original_id: overview
---

Liftbridge is a server that implements a durable, replicated, and scalable
message log. The vision for Liftbridge is to provide a "Kafka-lite" solution
designed with the [Go](https://go.dev) community first in mind. Unlike Kafka,
which is built on the JVM and whose canonical client library is Java (or the
C-based librdkafka), Liftbridge and its canonical client,
[go-liftbridge](https://github.com/liftbridge-io/go-liftbridge), are
implemented in Go. The ultimate goal of Liftbridge is to provide a lightweight
message-streaming solution with a focus on simplicity and usability.

To this end, Liftbridge was designed to bridge the gap between sophisticated
but complex log-based messaging systems like Apache Kafka and Apache Pulsar and
simpler, cloud-native solutions. There is no ZooKeeper or other unwieldy
dependencies, no JVM, no complicated API or configuration, and client libraries
are implemented using [gRPC](https://grpc.io).

Liftbridge is implemented on top of [NATS](https://nats.io), a lightweight,
high-performance pub/sub messaging system. This means it can be added to an
existing NATS deployment to provide message durability with no code changes. If
you are not already using NATS or not familiar with it, Liftbridge can be
deployed with NATS as an implementation detail.

Streams in Liftbridge are partitioned for horizontal scalability. By default,
streams have a single partition. Each partition has a leader and is replicated
to some set of followers for fault-tolerance. [Consumer groups](./consumer_groups.md)
provide means for implementing distributed, scalable, and fault-tolerant stream
processing with consumer balancing, consumer position tracking, automatic
consumer failover.
