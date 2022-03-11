---
id: version-v1.8.0-quick-start
title: Quick Start
original_id: quick-start
---

There are three ways to get started running Liftbridge on your machine:
[downloading a pre-built binary](#binary), [building from source](#building-from-source),
or [running a Docker container](#docker-container). There are also several
options for running a Liftbridge cluster [described below](#running-a-liftbridge-cluster-locally).

## Binary

A pre-built Liftbridge binary can be downloaded for a specific platform from
the [releases](https://github.com/liftbridge-io/liftbridge/releases) page. Once
you have installed the binary, refer to the steps under [Building From
Source](#building-from-source) for running the server.

## Building From Source

A Liftbridge binary can be built and installed from source using
[Go](https://golang.org/doc/install). Follow the below step to install from
source.

```shell
$ go get github.com/liftbridge-io/liftbridge
```
*Liftbridge uses [Go modules](https://github.com/golang/go/wiki/Modules), so
ensure this is enabled, e.g. `export GO111MODULE=on`.*

Liftbridge relies on [NATS](https://github.com/nats-io/nats-server) for
intra-cluster communication. By default, it will connect to a NATS server
running on localhost. The `--nats-servers` flag allows configuring the NATS
server(s) to connect to. Liftbridge can also run a NATS server [embedded in the
process](./embedded_nats.md), allowing it to run as a complete, self-contained
system with no external dependencies. The `--embedded-nats` flag will start an
embedded NATS server.

Also note that Liftbridge is clustered by default and relies on Raft for
coordination. This means a cluster of three or more servers is normally run
for high availability, and Raft manages electing a leader. A single server is
actually a cluster of size 1. For safety purposes, the server cannot elect
itself as leader without using the `--raft-bootstrap-seed` flag, which will
indicate to the server to elect itself as leader. This will start a single
server that can begin handling requests. **Use this flag with caution as it should
only be set on one server when bootstrapping a cluster.**

```shell
$ liftbridge --raft-bootstrap-seed --embedded-nats
INFO[2022-03-11 13:31:30] Liftbridge Version:        v1.8.0
INFO[2022-03-11 13:31:30] Server ID:                 YymOUlm62FeQbn32I3OWSn
INFO[2022-03-11 13:31:30] Namespace:                 liftbridge-default
INFO[2022-03-11 13:31:30] NATS Servers:              [nats://127.0.0.1:4222]
INFO[2022-03-11 13:31:30] Default Retention Policy:  [Age: 1 week, Compact: false]
INFO[2022-03-11 13:31:30] Default Partition Pausing: disabled
INFO[2022-03-11 13:31:30] Starting embedded NATS server on 0.0.0.0:4222
INFO[2022-03-11 13:31:30] Starting Liftbridge server on 0.0.0.0:9292...
INFO[2022-03-11 13:31:32] Server became metadata leader, performing leader promotion actions
```

Once a leader has been elected, other servers will automatically join the cluster.
We set the `--data-dir` and `--port` flags to avoid clobbering the first server.

```shell
$ liftbridge --data-dir /tmp/liftbridge/server-2 --port=9293
INFO[2022-03-11 13:32:26] Liftbridge Version:        v1.8.0
INFO[2022-03-11 13:32:26] Server ID:                 n9c07sbz0vaVH22HjAyhJw
INFO[2022-03-11 13:32:26] Namespace:                 liftbridge-default
INFO[2022-03-11 13:32:26] NATS Servers:              [nats://127.0.0.1:4222]
INFO[2022-03-11 13:32:26] Default Retention Policy:  [Age: 1 week, Compact: false]
INFO[2022-03-11 13:32:26] Default Partition Pausing: disabled
INFO[2022-03-11 13:32:26] Starting Liftbridge server on 0.0.0.0:9293...
```

We can also bootstrap a cluster by providing the explicit cluster configuration.
To do this, we provide the IDs of the participating peers in the cluster using the
`--raft-bootstrap-peers` flag. Raft will then handle electing a leader.

```shell
$ liftbridge --raft-bootstrap-peers server-2,server-3
```

## Docker Container

Instead of running a binary, you can run Liftbridge using a container. There is
a [container image](https://hub.docker.com/r/liftbridge/standalone-dev)
available which runs an instance of Liftbridge and an embedded NATS server
inside a single Docker container. This is meant for development and testing
purposes. Use the following Docker commands to run this container:

```shell
$ docker pull liftbridge/standalone-dev
$ docker run -d --name=liftbridge-main -p 4222:4222 -p 9292:9292 -p 8222:8222 -p 6222:6222 liftbridge/standalone-dev
```

This will run the container which will start both the NATS and Liftbridge
servers. To check the logs to see if the container started properly, run:

```shell
$ docker logs liftbridge-main
```

See the [deployment guide](./deployment.md) for more information.

## Running a Liftbridge Cluster Locally

The quickest way to get a multi-node Liftbridge cluster up and running on your
machine is with either [Docker Compose](https://docs.docker.com/compose) or
[Kind](https://kind.sigs.k8s.io) (Kubernetes in Docker). Follow the
[deployment guide](./deployment.md) for help running a cluster locally for
development or testing.
