---
id: version-v1.0.0-pausing-streams
title: Pausing Streams
original_id: pausing-streams
---

Liftbridge streams can be *paused* to conserve system resources such as CPU,
memory, and file descriptors. While pausing is performed on streams, the
granularity is specified at the partition level. We can pause all or a subset
of a stream's partitions. A partition is resumed when it is published to via
the Liftbridge `Publish` gRPC endpoint or if the stream was paused with
`ResumeAll` enabled and another partition in the stream was published to.

> **Use Case Note**
>
> Stream pausing can be useful for cases that involve a significant number of
> streams with only a small fraction being active at any given point in time,
> i.e. "sparse" streams. With the [activity stream](./activity.md), consumers
> can dynamically spin down when partitions are paused and spin back up once
> they are resumed.

Pause functionality is exposed through the Liftbridge [gRPC
API](https://github.com/liftbridge-io/liftbridge-api/blob/master/api.proto).
The `PauseStream` endpoint takes a `PauseStreamRequest` which specifies the
stream and set of partitions to pause. If no partitions are specified, the
operation will pause _all_ of the stream's partitions. Additionally, the
request includes a `ResumeAll` flag which indicates if all partitions should be
resumed when one is published to or only the partition that was published to.

When a partition is paused, the server will step down as leader or follower,
unsubscribe from the NATS subject, and close the commit log. This means
replication will stop, messages will not be received on the NATS subject, and
any file handles associated with the partition will be closed.

Pausing is maintained across server restarts.
