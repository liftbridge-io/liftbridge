---
id: version-v1.8.0-scalability-configuration
title: Configuring for Scalability
original_id: scalability-configuration
---

Liftbridge is designed to be clustered and horizontally scalable, meaning nodes
can be added to handle additional load. There are two dimensions to this
scalability, the control plane and the data plane. The _control plane_ refers
to the [metadata controller](./concepts.md#controller) which performs
operations and coordination responsibilies for the cluster. This is implemented
using the [Raft consensus algorithm](https://raft.github.io). The _data plane_
refers to the processes around actual messages and
[streams](./concepts.md#streams-and-partitions). It is important to understand
how the scalability of these two concerns interrelate.

## Scaling the Data Plane

There are a few different ways streams can be scaled in Liftbridge. These
different approaches are discussed [here](./concepts.md#scalability). To
summarize, both stream data and consumption can be scaled horizontally by
adding additional nodes to the cluster along with additional streams or
[stream partitioning](./concepts.md#streams-and-partitions). However, adding
nodes to the cluster has implications with the metadata Raft cluster used by
the control plane. This is discussed below.

Stream partitioning provides increased parallelism which allows for greater
throughput of messages. [Consumer groups](./consumer_groups.md) provide a means
for coordinating and balancing the consumption of partitions across a set of
consumers.

## Scaling the Control Plane

By default, new servers that are added to a Liftbridge cluster participate in
the Raft consensus group used by the control plane. These are referred to as
_voters_. This means they are involved in elections for the metadata leader and
committing entries to the Raft log. However, because Raft requires a minimum of
`N/2+1` nodes to perform operations, this can severely limit the scalability of
the control plane. For example, in a 100-node cluster, 51 nodes have to respond
to commit an operation. Additionally, the Raft protocol requires exchanging
`N^2` messages to arrive at consensus for a given operation.

To address this, Liftbridge has a setting to limit the number of voters who
participate in the Raft group. The [`clustering.raft.max.quorum.size`](./configuration.md#clustering-configuration-settings)
setting restricts the number of servers who participate in the Raft quorum. Any
servers added to the cluster beyond this number participate as _non-voters_.
Non-voter servers operate as normal but are not involved in the Raft election
or commitment processes. By default, `clustering.raft.max.quorum.size` is set
to `0`, which means there is no limit. Limiting this number allows the control
plane to better scale as nodes are added. This is typically not a concern for
smaller clusters, such as single-digit or low-double-digit clusters but can be
an issue for clusters beyond these sizes. This configuration should be set to
the same value on all servers in the cluster.

The configuration example below shows how to limit the Raft quorum to five
servers.

```yaml
clustering:
  raft.max.quorum.size: 5
```

Guidance on cluster and quorum size is use-case specific, but it is recommended
to specify an odd number for `clustering.raft.max.quorum.size` (or to run an
odd number of servers if not limiting quorum size), e.g. 3 or 5, depending on
scaling needs. Ideally, cluster members are run in different availability zones
or racks for improved fault-tolerance.
