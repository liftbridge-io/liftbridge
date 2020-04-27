![Liftbridge Logo](./website/static/img/liftbridge_full.png)
---
[![Build][Build-Status-Image]][Build-Status-Url] [![License][License-Image]][License-Url] [![ReportCard][ReportCard-Image]][ReportCard-Url] [![Coverage][Coverage-Image]][Coverage-Url]

Liftbridge provides lightweight, fault-tolerant message streams by implementing
a durable stream augmentation for the [NATS messaging system](https://nats.io).
It extends NATS with a Kafka-like publish-subscribe log API that is highly
available and horizontally scalable. The goal of Liftbridge is to provide a
message-streaming solution with a focus on simplicity and usability. Use it as
a simpler and lighter alternative to systems like Kafka and Pulsar or to add
streaming semantics to an existing NATS deployment.

See the [introduction](https://bravenewgeek.com/introducing-liftbridge-lightweight-fault-tolerant-message-streams/)
post on Liftbridge and [this post](https://bravenewgeek.com/building-a-distributed-log-from-scratch-part-5-sketching-a-new-system/)
for more context and some of the inspiration behind it.

## Documentation

- [Official documentation](https://liftbridge.io/docs/overview.html)
- [Getting started](https://liftbridge.io/docs/quick-start.html)
- [FAQ](https://liftbridge.io/docs/faq.html)
- [Website](https://liftbridge.io)

## Community

- [Slack](https://liftbridge.slack.com) - click [here](https://liftbridge.io/help.html) to request an invite
- [Twitter](https://twitter.com/liftbridge_io)


[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[Build-Status-Url]: https://circleci.com/gh/liftbridge-io/liftbridge
[Build-Status-Image]: https://circleci.com/gh/liftbridge-io/liftbridge.svg?style=svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/liftbridge-io/liftbridge
[ReportCard-Image]: https://goreportcard.com/badge/github.com/liftbridge-io/liftbridge
[Coverage-Url]: https://coveralls.io/github/liftbridge-io/liftbridge?branch=master
[Coverage-image]: https://coveralls.io/repos/github/liftbridge-io/liftbridge/badge.svg?branch=master
