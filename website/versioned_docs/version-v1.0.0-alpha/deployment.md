---
id: version-v1.0.0-alpha-deployment
title: Deployment
original_id: deployment
---

## Development / Testing

In addition to running a native binary as described in the [quick start
guide](./quick_start.md), there are three options for running a Liftbridge
cluster locally for development and testing purposes:

- [Docker](https://www.docker.com) - single-node Liftbridge cluster backed by a
  single-node NATS cluster in one container
- [Docker Compose](https://docs.docker.com/compose) - three-node Liftbridge
  cluster backed by a single-node NATS cluster in separate containers
- [Kind](https://kind.sigs.k8s.io) (Kubernetes in Docker) - three-node
  Liftbridge cluster backed by a three-node NATS cluster running inside a local
  Kubernetes cluster

### Docker

There is a [container image](https://hub.docker.com/r/liftbridge/liftbridge-docker)
available which runs an instance of Liftbridge and NATS inside a [single Docker
container](https://github.com/liftbridge-io/liftbridge-docker) for development
and testing purposes. In effect, this runs a single-node Liftbridge cluster on
your machine.

Use the following Docker commands to run the container:

```shell
$ docker pull liftbridge/liftbridge-docker
$ docker run -d --name=liftbridge-main -p 4222:4222 -p 9292:9292 -p 8222:8222 -p 6222:6222 liftbridge/liftbridge-docker
```

This will run the container which will start both the NATS and Liftbridge
servers. To check the logs to see if the container started properly, run:

```shell
$ docker logs liftbridge-main
```

When running the container, you can optionally specify the mount point with:

`--volume=/tmp/host/liftbridge:/tmp/liftbridge/liftbridge-default`

This container exposes several ports described below.

Liftbridge server exposes:
- 9292 for clients connections

NATS server exposes:
- 4222 for clients connections
- 8222 as an HTTP management port for information reporting and monitoring
- 6222 is a routing port for clustering

### Docker Compose

This will bring up three Liftbridge containers and one NATS node using Docker
Compose:

```shell
$ make compose-up
```

To tear it down, run:

```shell
$ make compose-down
```

### Kind

This will deploy a three-node Liftbridge cluster backed by a three-node NATS
cluster locally using Kind. For this you'll also need
[Skaffold](https://skaffold.dev) and [Kustomize](https://kustomize.io) in
addition to Kind.

Download them:
- [kind](https://github.com/kubernetes-sigs/kind/releases) version 0.5.1 or
  above
- [kustomize](https://github.com/kubernetes-sigs/kustomize/releases) version
  3.3.0 or above
- [skaffold](https://skaffold.dev/docs/install/) version 0.41.0 or later

To provision a local Kind cluster, run:

```shell
$ make kind-up
```

Then deploy the manifests including the NATS operator and the Liftbridge
cluster with:

```shell
$ make kind-apply
```

To export the `KUBECONFIG` environment variable and point kubectl to the right
context, run:

```shell
$ make kind-export
```

After running this, you can then use kubectl as normal to interact with the
local Kubernetes cluster.

To tear down your local environment, run:

```shell
$ make kind-down
```

## Production

We'll provide guidance on production deployments when a 1.0 release rolls
around.
