# liftbridge

Liftbridge provides lightweight, fault-tolerant message streams by implementing
a durable stream augmentation for the [NATS messaging system](https://nats.io).
It extends NATS with a Kafka-like publish-subscribe log API that is highly
available and horizontally scalable. Use Liftbridge as a simpler and lighter
alternative to systems like Kafka and Pulsar or use it to add streaming
semantics to an existing NATS deployment.

See [this
post](https://bravenewgeek.com/building-a-distributed-log-from-scratch-part-5-sketching-a-new-system/)
for more context and some of the inspiration behind Liftbridge.

## Key Features

- Log-based API for NATS
- Replicated for fault-tolerance
- Horizontally scalable
- Wildcard subscription support
- At-least-once delivery support
- Message key-value support
- Log compaction by key (WIP) 
- Single static binary (~16MB)
- Designed to be high-throughput (more on this to come)
- Supremely simple

## FAQ

### What is Liftbridge?

Liftbridge is a server that implements a durable, replicated message log for
[NATS](https://github.com/nats-io/gnatsd). Clients create a named *stream*
which is attached to a NATS subject. The stream then records messages on that
subject to a replicated write-ahead log. Multiple consumers can read back
from the same stream, and multiple streams can be attached to the same
subject. Liftbridge provides a Kafka-like API in front of NATS.

### Why was it created?

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

### Why not NATS Streaming?

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

### How does it scale?

Liftbridge scales horizontally by adding more brokers to the cluster and
creating more streams which are distributed among the cluster. In effect, this
splits out message routing from storage and consumption, which allows
Liftbridge to scale independently and eschew subject partitioning.
Alternatively, streams can join a load-balance group, which effectively load
balances a NATS subject among the streams in the group without affecting
delivery to other streams.

### What about HA?

High availability is achieved by replicating the streams. When a stream is
created, the client specifies a `replicationFactor`, which determines the
number of brokers to replicate the stream. Each stream has a leader who is
responsible for handling reads and writes. Followers then replicate the log
from the leader. If the leader fails, one of the followers can set up to
replace it. The replication protocol closely resembles that of Kafka, so there
is much more nuance to avoid data consistency problems. This will be documented
in more detail in the near future.

### Is it production-ready?

No, this project is early and still evolving.

## Installation

Liftbridge uses [dep](https://github.com/golang/dep) to vendor dependencies.
If you don't have dep installed, run the following:

```
$ brew install dep
```

To build and install Liftbridge from source, run the following:

```
$ go get github.com/tylertreat/liftbridge
$ cd $GOPATH/src/github.com/tylertreat/liftbridge
$ dep ensure
$ go install
```

## Quick Start

Liftbridge currently relies on an externally running
[NATS server](https://github.com/nats-io/gnatsd). By default, it will connect
to a NATS server running on localhost. The `--nats-server` flag allows
configuring the NATS server(s) to connect to.

Also note that Liftbridge is clustered by default and relies on Raft for
coordination. This means a cluster of three or more servers is normally run
for high availability, and Raft manages electing a leader. A single server is
actually a cluster of size 1. For safety purposes, the server cannot elect
itself as leader without using the `--raft-bootstrap-seed` flag, which will
indicate to the server to elect itself as leader. This will start a single
server that can begin handling requests. **Use this flag with caution as it should
only be set on one server when bootstrapping a cluster.**

```
$ liftbridge --raft-bootstrap-seed
INFO[2018-07-05 16:29:44] Server ID: kn3MGwCL3TKRNyGS9bZLgH
INFO[2018-07-05 16:29:44] Namespace: liftbridge-default
INFO[2018-07-05 16:29:44] Starting server on :9292...
INFO[2018-07-05 16:29:46] Server became metadata leader, performing leader promotion actions
```

Once a leader has been elected, other servers will automatically join the cluster.
We set the `--data-dir` and `--port` flags to avoid clobbering the first server.

```
$ liftbridge --data-dir /tmp/liftbridge/server-2 --port=9293
INFO[2018-07-05 16:39:21] Server ID: 32CpplyaA031EFEW1DQzx6
INFO[2018-07-05 16:39:21] Namespace: liftbridge-default
INFO[2018-07-05 16:39:21] Starting server on :9293...
```

We can also bootstrap a cluster by providing the explicit cluster configuration.
To do this, we provide the IDs of the participating peers in the cluster using the
`--raft-bootstrap-peers` flag. Raft will then handle electing a leader.

```
$ liftbridge --raft-bootstrap-peers server-2,server-3
```

## Client Libraries

Currently, there is only a high-level
[Go client library](https://github.com/tylertreat/go-liftbridge) available.
However, Liftbridge uses gRPC for its client API, so client libraries can
be generated quite easily using the
[Liftbridge protobuf definitions](https://github.com/tylertreat/liftbridge-grpc).

## TODO

- [ ] Production-hardening
- [ ] TLS support
- [ ] Configurable acks
- [ ] Log retention by message age
- [ ] Log compaction by key
- [ ] Consumer-offset checkpointing in the log
- [ ] Minimum ISR support
- [ ] Additional subscribe semantics
  - [ ] Oldest
  - [ ] Newest
  - [ ] New messages only
  - [ ] By timestamp
  - [ ] By time delta
- [ ] Single-stream fanout
  - [ ] Opt-in ISR replica reads
  - [ ] Read-replica support
- [ ] Authentication and authorization
- [ ] Embedded NATS server option
- [ ] Better instrumentation/observability

## Acknowledgements

- [Derek Collison](https://twitter.com/derekcollison) and NATS team for
  building NATS and NATS Streaming and providing lots of inspiration.
- [Travis Jeffery](https://twitter.com/travisjeffery) for building
  [Jocko](https://github.com/travisjeffery/jocko), a Go implementation of
  Kafka. The Liftbridge log implementation builds heavily upon the commit log
  from Jocko.
- [Apache Kafka](http://kafka.apache.org) for inspiring large parts of the
  design, particularly around replication.
