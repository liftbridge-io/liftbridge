---
id: overview
title: Liftbridge Overview
---

Liftbridge is a server that implements a durable, replicated message log for
[NATS](https://nats.io). Clients create a named stream which is attached to a
NATS subject. The stream then records messages on that subject to a replicated
write-ahead log. Multiple consumers can read back from the same stream, and
multiple streams can be attached to the same subject. Liftbridge provides a
Kafka-like API in front of NATS.

Liftbridge streams are partitioned for horizontal scalability. By default,
streams have a single partition. Each partition maps to a separate NATS
subject derived from the stream subject. Each has a leader and is replicated
to some set of followers for fault-tolerance.

NATS is a lightweight, high-performance pub/sub messaging system. It differs
from traditional messaging middleware in that it does not provide queuing or
message durability. Instead, NATS is a fire-and-forget system with at-most-once
delivery semantics. This allows it to be extremely lightweight, highly
scalable, and—above all—dead simple.

While NATS aims to be the always-on dial tone for cloud-native systems,
Liftbridge is the voicemail. It provides an immutable, append-only log of NATS
messages. Unlike a message queue where consumers *remove* messages from a
queue, with Liftbridge, consumers read back the log. In this sense, the design
is very similar to [Apache Kafka](http://kafka.apache.org/).

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

The high-level data flow in Liftbridge is shown in the diagram below.

![high-level data flow](assets/high-level.png)
