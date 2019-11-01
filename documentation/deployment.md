---
id: deployment
title: Deployment
---

## Development / Testing

There are currently two options for running a Liftbridge cluster locally for
development or testing purposes: [Docker
Compose](https://docs.docker.com/compose) or [Kind](https://kind.sigs.k8s.io)
(Kubernetes in Docker).

### Docker Compose based

This will bring up three Liftbridge containers and one NATS node:

```shell
$ make compose-up
```

To tear it down, run:

```shell
$ make compose-down
```

### kind-based

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
