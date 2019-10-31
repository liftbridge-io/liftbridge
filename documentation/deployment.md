---
id: deployment
title: Deployment
---

## Development / Testing

There's currently two options for running a Liftbridge cluster locally or for testing.

### Docker Compose based

```shell
$ make compose-up
```

That will bring up 3 Liftbridge containers and one NATS node.

To tear it down:
```shell
$ make compose-down
```

### kind-based

This will deploy a 3-node Liftbridge cluster backed by a 3-node NATS cluster locally using kind. For this you'll also need skaffold and kustomize in addition to kind.

Download them:
- [kind](https://github.com/kubernetes-sigs/kind/releases) version 0.5.1 or above
- [kustomize](https://github.com/kubernetes-sigs/kustomize/releases) version 3.3.0 or above
- [skaffold](https://skaffold.dev/docs/install/) version 0.41.0 or later

Then run
```shell
$ make kind-up
```

That will provision a local kind cluster.

```shell
$ make kind-apply
```

Will deploy the manifests including the NATS operator and the Liftbridge cluster.

Run
```shell
$ make kind-export
```
to get the command to run to export the `KUBECONFIG` env variable and point kubectl to the right context.

To tear down your local environment:
```shell
$ make kind-down
```

## Production

We'll provide guidance on production deployments when a 1.0 release rolls around.
