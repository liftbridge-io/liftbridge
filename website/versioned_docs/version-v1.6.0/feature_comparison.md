---
id: version-v1.6.0-feature-comparison
title: Feature Comparison
original_id: feature-comparison
---

Liftbridge shares some similarities with other stream-oriented pub/sub
messaging systems. The below feature-comparison matrix shows how it compares to
a few of these systems. 

> **Architect's Note**
>
> These are highly nuanced pieces of infrastructure, so a straight comparison
> of features is often of minimal value when evaluating solutions. This matrix
> is primarily meant to serve as a reference point for those attempting to
> develop a mental model of Liftbridge.

|  | Liftbridge | NATS Streaming | Apache Kafka | Apache Pulsar |
|:----|:----|:----|:----|:----|
| Runtime | self-contained binary | self-contained binary | JVM | JVM |
| At-least-once delivery | ✓ | ✓ | ✓ | ✓ |
| Transactions |  |  | ✓ |  |
| Partitioning | ✓ |  | ✓ | ✓ |
| Linear Horizontal Scalability | ✓ |  | ✓ | ✓ |
| Clustering/Replication | ✓ | ✓ | ✓ | ✓ |
| External Coordination |  |  | Apache ZooKeeper | Apache ZooKeeper |
| Message Replay | ✓ | ✓ | ✓ | ✓ |
| Message Queueing |  | ✓ |  | ✓ |
| Wildcard Topic Matching | ✓ | |  | |
| Topic Pausing | ✓ | |  | |
| Rate Limiting |  | opt-in rate matching | quotas | publish rate limiting |
| Log Retention | ✓ | ✓ | ✓ | ✓ |
| Log Compaction | ✓ |  | ✓ | ✓ |
| Message Headers | ✓ |  | ✓ | ✓ |
| Multiple Consumers | independent subscribers | queue subscriptions | consumer groups | subscriptions |
| Consumer Position Tracking | cursors | durable subscriptions | consumer groups | cursors |
| Event-enabled | activity stream | | | |
| Multitenancy | namespaced clusters | namespaced clusters | topic-level ACLs | multitenant shared cluster |
| Authentication | TLS | User authentication, TLS | TLS, Kerberos, SASL | TLS, Kerberos, JWT, Athenz |
| Authorization | | | ACLs | ACLs |
| Storage | filesystem | memory, filesystem, SQL| filesystem | filesystem (Apache BookKeeper), tiered storage (Amazon S3, Google Cloud Storage) |
| Encryption of data-at-rest | ✓ | ✓| ✓ | ✓ |