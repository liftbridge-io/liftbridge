![Liftbridge Logo](./website/static/img/liftbridge_full.png)
---
[![Build][Build-Status-Image]][Build-Status-Url] [![License][License-Image]][License-Url] [![ReportCard][ReportCard-Image]][ReportCard-Url] [![Go Version](https://img.shields.io/badge/go-1.25.3-blue)](https://go.dev/)

Liftbridge provides lightweight, fault-tolerant message streams by implementing
a durable, replicated, and scalable message log. The vision for Liftbridge is
to deliver a "Kafka-lite" solution designed with the Go community first in
mind. Unlike Kafka, which is built on the JVM and whose canonical client
library is Java (or the C-based librdkafka), Liftbridge and its canonical
client, [go-liftbridge](https://github.com/liftbridge-io/go-liftbridge), are
implemented in Go. The ultimate goal of Liftbridge is to provide a lightweight
message-streaming solution with a focus on simplicity and usability. Use it as
a simpler and lighter alternative to systems like Kafka and Pulsar or to add
streaming semantics to an existing NATS deployment.

See the [introduction](https://bravenewgeek.com/introducing-liftbridge-lightweight-fault-tolerant-message-streams/)
post on Liftbridge and [this post](https://bravenewgeek.com/building-a-distributed-log-from-scratch-part-5-sketching-a-new-system/)
for more context and some of the inspiration behind it.

## Maintainers

This project is maintained by [Basekick Labs](https://github.com/basekick-labs), creators of [Arc](https://github.com/basekick-labs/arc).

## Requirements

- Go 1.25.3+
- NATS Server v2.10.0+

## Documentation

- [Official documentation](https://liftbridge.io/docs/overview.html)
- [Getting started](https://liftbridge.io/docs/quick-start.html)
- [FAQ](https://liftbridge.io/docs/faq.html)
- [Website](https://liftbridge.io)

## Community

- [Discord](https://discord.gg/nxnWfUxsdm)


[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[Build-Status-Url]: https://github.com/liftbridge-io/liftbridge/actions/workflows/test.yml
[Build-Status-Image]: https://github.com/liftbridge-io/liftbridge/actions/workflows/test.yml/badge.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/liftbridge-io/liftbridge
[ReportCard-Image]: https://goreportcard.com/badge/github.com/liftbridge-io/liftbridge
