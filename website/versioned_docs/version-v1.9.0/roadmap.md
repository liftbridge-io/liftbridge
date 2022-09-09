---
id: version-v1.9.0-roadmap
title: Product Roadmap
original_id: roadmap
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

## H1 2022

### ~~Consumer Groups ([#46](https://github.com/liftbridge-io/liftbridge/issues/46))~~

Provide high-level consumer functionality to allow for durable subscriptions,
balanced stream consumption, and fault-tolerant consumers. The first step to
this will be providing support for single-member consumer groups, which will
effectively provide a durable subscription, then generalizing to multi-member
groups. The latter will require a mechanism for group coordination.

### ~~Authentication and Authorization ([#36](https://github.com/liftbridge-io/liftbridge/issues/36))~~

Provide mechanisms for client identity and authentication as well as granular
access control. These will likely be delivered as separate units of work with
authentication providing the groundwork for fine-grained authorization. As a
result, these may be broken out into separate roadmap items in the near future.

## H2 2022

### Monitoring API ([#222](https://github.com/liftbridge-io/liftbridge/issues/222))

Provide an API that exposes monitoring information and metrics about the server
to better support Liftbridge operations. This paves the way for future
monitoring and observability integrations.

### Authorization Enhancements ([#409](https://github.com/liftbridge-io/liftbridge/issues/409)

Implement syncing of authorization policies across cluster nodes. This improves
operator experience by not requiring policy files to be modified across all
nodes in a cluster. It also prevents permissions drift across cluster nodes.

## H1 2023

### Optional NATS API ([#221](https://github.com/liftbridge-io/liftbridge/issues/221))

With the introduction of [Embedded NATS Server](#embedded-nats-server-19httpsgithubcomliftbridge-ioliftbridgeissues19),
allow the NATS API to be exposed optionally. This, in effect, turns NATS into
an implementation detail and allows users to just interact with the Liftbridge
API as a standalone service.

### Tiered Storage ([#110](https://github.com/liftbridge-io/liftbridge/issues/110))

Provide support for transparent offloading of log segments to object storage,
such as Amazon S3, so that brokers only need to keep open (or some configurable
tail of) segments locally on disk. When a consumer needs to read older
segments, the broker or, potentially, the client transparently reads from
object storage.

### Federated Clustering ([#219](https://github.com/liftbridge-io/liftbridge/issues/219))

Implement federated Liftbridge clustering to better support geo-replication.
This may involve implementing a system similar to Kafka MirrorMaker for
replicating data between clusters and/or leveraging NATS superclustering and
geo-aware subscribers.

## H2 2023

### Kafka Bridge Connector ([#220](https://github.com/liftbridge-io/liftbridge/issues/220))

Implement a bridge process that maps Kafka topics to Liftbridge streams to
better support the migration from Kafka to Liftbridge or to support hybrid
scenarios.

### Flatbuffers and Zero-Copy Support ([#87](https://github.com/liftbridge-io/liftbridge/issues/87), [#185](https://github.com/liftbridge-io/liftbridge/issues/185))

Provide opt-in support for Flatbuffers and a zero-copy API for high-performance
use cases.
