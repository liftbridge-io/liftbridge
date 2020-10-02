---
id: roadmap
title: Product Roadmap
---

This roadmap is not intended to provide a firm timeline or commitment of
features and capabilities, nor does it include the comprehensive set of planned
work. Its purpose is to offer visibility into the major items that are planned
and the rough sequencing of them. It also is intended to lay out the vision and
direction of the project. Items not included in the roadmap may still be
implemented. Similarly, items in the roadmap may end up not being implemented
or may look different than as described here. As such, the roadmap exists as a
living document and will be updated as things evolve.

Please [create an issue](https://github.com/liftbridge-io/liftbridge/issues/new)
to provide input on the roadmap or refer to [existing issues](https://github.com/liftbridge-io/liftbridge/issues)
to comment on particular roadmap items.

## H2 2020

### ~~Stream-Level Configuration ([#75](https://github.com/liftbridge-io/liftbridge/issues/75))~~

Allow users to configure and override server defaults for individual streams
via the gRPC API. To date, streams are configured globally using the Liftbridge
configuration.

### ~~Asynchronous Publish API ([#215](https://github.com/liftbridge-io/liftbridge/issues/215))~~

The current publish endpoint is synchronous, meaning each publish is a full
server roundtrip. To enable higher throughput of messages, implement an
asynchronous publish endpoint that allows for pipelined publishes. This
endpoint will ultimately replace the synchronous endpoint since the synchronous
behavior can be implemented on top of the async API. At this point, the
synchronous endpoint will be deprecated.

### ~~Automatic Partition Pausing ([#218](https://github.com/liftbridge-io/liftbridge/issues/218))~~

Allow partitions to be automatically paused when they go idle to support large
numbers of sparse streams. Users can configure the max time with no messages
before a partition is paused at the stream level or globally. This is needed to
enable the [Consumer Position Tracking API](#consumer-position-tracking-api-214httpsgithubcomliftbridge-ioliftbridgeissues214)
such that consumer offsets can be stored in an internal, partitioned stream
efficiently. 

### ~~Command-Line Tool ([#249](https://github.com/liftbridge-io/liftbridge/issues/249))~~

Provide a CLI tool for interacting with a Liftbridge cluster in order to
perform basic operational tasks including performance evaluation, maintenance,
and statistics gathering.

### ~~Consumer Position Tracking API ([#214](https://github.com/liftbridge-io/liftbridge/issues/214))~~

Provide a means for “durable” stream subscriptions which allows consumers to
maintain their position within a stream without having to manage the positions
themselves. This initial work involves implementing a low-level API for storing
and retrieving stream cursors to allow manual checkpointing. This set of
primitives will enable higher-level functionality like [Consumer
Groups](#consumer-groups-46httpsgithubcomliftbridge-ioliftbridgeissues46),
which will provide managed checkpointing and consumer coordination.

### Consumer Groups ([#46](https://github.com/liftbridge-io/liftbridge/issues/46))

Provide high-level consumer functionality to allow for durable subscriptions,
balanced stream consumption, and fault-tolerant consumers. The first step to
this will be providing support for single-member consumer groups, which will
effectively provide a durable subscription, then generalizing to multi-member
groups. The latter will require a mechanism for group coordination.

### ~~Expose Offset Information in Metadata ([#111](https://github.com/liftbridge-io/liftbridge/issues/111))~~

Include partition offset information, such as the log-end offset (LEO) and high
watermark (HW), in cluster metadata. This information is useful in a variety of
use cases, such as allowing single-producer streams to know when the
end-of-stream has been reached and to support [Optimistic Concurrency
Control](#optimistic-concurrency-control-54httpsgithubcomliftbridge-ioliftbridgeissues54).

### Optimistic Concurrency Control ([#54](https://github.com/liftbridge-io/liftbridge/issues/54))

Implement a CAS primitive for optimistic concurrency control on publishes. This
enables several use cases where there is one (or a small number) of publishers
for a given partition, such as idempotent publishes, monotonic writes, and
transaction logs.

## H1 2021

### Monitoring API ([#222](https://github.com/liftbridge-io/liftbridge/issues/222))

Provide an API that exposes monitoring information and metrics about the server
to better support Liftbridge operations. This paves the way for future
monitoring and observability integrations.

### Embedded NATS Server ([#19](https://github.com/liftbridge-io/liftbridge/issues/19))

Allow the Liftbridge server to run an embedded instance of NATS rather than
relying on an external NATS server. This allows Liftbridge to run as a fully
self-contained process without the need for external dependencies. It also lays
the groundwork for [Optional NATS API](#optional-nats-api-221httpsgithubcomliftbridge-ioliftbridgeissues221)
which turns NATS into an opt-in implementation detail. Eventually, Liftbridge
can be used fully on its own without NATS by allowing clients to transition to
the Liftbridge API.

### Quorum Size Limit ([#41](https://github.com/liftbridge-io/liftbridge/issues/41))

Currently, all servers in a cluster participate in the metadata Raft quorum.
This severely limits scalability of the cluster. Allow having a subset of
servers form the Raft quorum to increase scalability. This involves having
non-quorum servers run as non-voting members of the cluster.

### Authentication and Authorization ([#36](https://github.com/liftbridge-io/liftbridge/issues/36))

Provide mechanisms for client identity and authentication as well as granular
access control. These will likely be delivered as separate units of work with
authentication providing the groundwork for fine-grained authorization. As a
result, these may be broken out into separate roadmap items in the near future.

### Encryption at Rest ([#217](https://github.com/liftbridge-io/liftbridge/issues/217))

Support encryption of stream data stored on disk. This may also include
encrypting metadata such as the Raft log.


## H2 2021

### Tiered Storage ([#110](https://github.com/liftbridge-io/liftbridge/issues/110))

Provide support for transparent offloading of log segments to object storage,
such as Amazon S3, so that brokers only need to keep open (or some configurable
tail of) segments locally on disk. When a consumer needs to read older
segments, the broker or, potentially, the client transparently reads from
object storage.

### Kafka Bridge Connector ([#220](https://github.com/liftbridge-io/liftbridge/issues/220))

Implement a bridge process that maps Kafka topics to Liftbridge streams to
better support the migration from Kafka to Liftbridge or to support hybrid
scenarios.

### Federated Clustering ([#219](https://github.com/liftbridge-io/liftbridge/issues/219))

Implement federated Liftbridge clustering to better support geo-replication.
This may involve implementing a system similar to Kafka MirrorMaker for
replicating data between clusters and/or leveraging NATS superclustering and
geo-aware subscribers.

### Optional NATS API ([#221](https://github.com/liftbridge-io/liftbridge/issues/221))

With the introduction of [Embedded NATS Server](#embedded-nats-server-19httpsgithubcomliftbridge-ioliftbridgeissues19),
allow the NATS API to be exposed optionally. This, in effect, turns NATS into
an implementation detail and allows users to just interact with the Liftbridge
API as a standalone service.

### Flatbuffers and Zero-Copy Support ([#87](https://github.com/liftbridge-io/liftbridge/issues/87), [#185](https://github.com/liftbridge-io/liftbridge/issues/185))

Provide opt-in support for Flatbuffers and a zero-copy API for high-performance
use cases.

