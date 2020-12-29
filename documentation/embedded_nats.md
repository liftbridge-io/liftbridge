---
id: embedded-nats
title: Embedded NATS
---

Liftbridge relies on [NATS](https://github.com/nats-io/nats-server) for
intra-cluster communication. By default, it will connect to a NATS server
running on localhost. The
[`nats.servers`](./configuration.md#nats-configuration-settings) setting allows
configuring the NATS server(s) to connect to. However, Liftbridge can also run
a NATS server embedded in the process, allowing it to run as a complete,
self-contained system with no external dependencies. The below examples show
how this can be done.

## Default Configuration

To run an embedded NATS server with the [default configuration](https://docs.nats.io/nats-server/configuration#configuration-properties),
simply enable the `embedded` setting in the [`nats`](./configuration.md#nats-configuration-settings)
configuration (or equivalent `--embedded-nats` command-line flag). Set
`logging.nats` to enable logging for the NATS server.

```yaml
nats:
  embedded: true

logging.nats: true
```

This will start a NATS server bound to `0.0.0.0:4222` with default settings.

## Custom Configuration

To run an embedded NATS server with custom configuration, use the
[`embedded.config`](./configuration.md#nats-configuration-settings) setting (or
equivalent `--embedded-nats-config` command-line flag) to specify a [NATS
configuration file](https://docs.nats.io/nats-server/configuration) to use. By
specifying this, the `embedded` setting will be automatically enabled. Set
`logging.nats` to enable logging for the NATS server.

```yaml
nats:
  embedded.config: nats.conf

logging.nats: true
```

This will start a NATS server using the specified configuration file.
