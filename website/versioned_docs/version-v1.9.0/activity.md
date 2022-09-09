---
id: version-v1.9.0-activity
title: Activity Stream
original_id: activity
---

Liftbridge exposes a stream of internal meta-events called the _Activity
Stream_. These events allow users to react to changes in the cluster, such as
streams being created or deleted. The activity stream is named `__activity`,
and it has a single partition. Events in the stream are encoded as protobuf
messages called an [`ActivityStreamEvent`](https://github.com/liftbridge-io/liftbridge-api/blob/master/api.proto).
The activity stream is disabled by default. See
[below](#configuring-the-activity-stream) for configuring the activity stream.

> **Use Case Note**
>
> The activity stream enables dynamically responding to events in the cluster.
> For example, imagine a situation where we need to manage a group of consumers
> that process events arising across a large collection of streams. The set of
> streams is dynamic, meaning streams are programmatically created and
> destroyed frequently. The activity stream allows us to be notified of newly
> created streams as well as deleted streams. With this, we can manage the
> process of consuming the streams as they come and go.
>
> Another use case might be using the activity stream for auditing purposes,
> e.g. providing an audit history of when streams are created, deleted, or
> modified or tracking consumer group activity.

Events in the activity stream are totally ordered with respect to the order in
which they were applied to the cluster. This also means the activity stream
enforces head-of-line blocking when events are published to the stream. For
example, imagine two operations are applied to the cluster in the given order:
_Create Stream Foo_ and _Delete Stream Foo_. If the activity stream is
unavailable for some reason when publishing the _Create Stream_ event, perhaps
because of a leadership transition, it will not attempt to publish any other
events until they are published in the order in which they occurred. If an
event publish fails, it will retry the publish with some backoff until it
succeeds. As a result, the activity stream ensures at-least-once delivery of
events.

The above delivery guarantees survive node failures in the cluster. This is
possible by designating an _activity manager_ which is piggybacked off of the
[controller](./concepts.md#controller) elected by Raft. This means, at any
given time, there is at most _one_ node responsible for publishing events.
However, as indicated above, it's possible for an event to be published more
than once. For example, if an event is published but the node acting as
activity manager fails before recording this fact, the newly elected activity
manager will re-publish the event.

To account for potential redeliveries, events include a unique and strictly
increasing ID. This ID can be used by clients to deduplicate events in the
activity stream.

## Supported Events

Below is the list of supported events and their fields in the activity stream.
Refer to [liftbridge-api](https://github.com/liftbridge-io/liftbridge-api) for
the protobuf definitions and code for deserializing these events. Client
libraries may also provide APIs for accessing the activity stream.

### Create Stream

Fired when a stream is created.

| Field | Type | Description |
|:----|:----|:----|
| id | unsigned int | Unique and strictly increasing event ID. |
| stream | string | The name of the stream that was created. |
| partitions | list of ints | The IDs of the partitions that were created. |

### Delete Stream

Fired when a stream is deleted.

| Field | Type | Description |
|:----|:----|:----|
| id | unsigned int | Unique and strictly increasing event ID. |
| stream | string | The name of the stream that was deleted. |

### Pause Stream

Fired when a stream is [paused](./pausing_streams.md).

| Field | Type | Description |
|:----|:----|:----|
| id | unsigned int | Unique and strictly increasing event ID. |
| stream | string | The name of the stream that was paused. |
| partitions | list of ints | The IDs of the partitions that were paused. |
| resumeAll | bool | The `ResumeAll` value on the pause request, indicating if all partitions should be resumed when one is published to or only the partition that was published to. |

### Resume Stream

Fired when one or more stream partitions are resumed.

| Field | Type | Description |
|:----|:----|:----|
| id | unsigned int | Unique and strictly increasing event ID. |
| stream | string | The name of the stream that has partitions that were resumed. |
| partitions | list of ints | The IDs of the partitions that were resumed. |

### Set Stream Readonly

Fired when the readonly flag is changed for one or more stream partitions.

| Field | Type | Description |
|:----|:----|:----|
| id | unsigned int | Unique and strictly increasing event ID. |
| stream | string | The name of the stream that has partitions which the readonly flag was modified for. |
| partitions | list of ints | The IDs of the partitions that the readonly flag was modified for. |
| readonly | bool | If the partitions were set to readonly or read-write. |

### Join Consumer Group

Fired when a consumer joins a [consumer group](./consumer_groups.md).

| Field | Type | Description |
|:----|:----|:----|
| id | unsigned int | Unique and strictly increasing event ID. |
| groupId | string | The ID of the consumer group being joined. |
| consumerId | string | The ID of the consumer joining the group. |
| streams | list of strings | The streams the consumer is subscribed to. |

### Leave Consumer Group

Fired when a consumer leaves a [consumer group](./consumer_groups.md) or is
removed due to timing out.

| Field | Type | Description |
|:----|:----|:----|
| id | unsigned int | Unique and strictly increasing event ID. |
| groupId | string | The ID of the consumer group being left. |
| consumerId | string | The ID of the consumer leaving the group. |
| expired | bool | Indicates if the consumer was removed due to timing out. |

## Configuring the Activity Stream

Configuration settings for the activity stream are grouped under the `activity`
namespace. See the full list of configuration options
[here](./configuration.md#activity-configuration-settings).

The activity stream is disabled by default. Use the following configuration to
enable it:

```yaml
activity.stream.enabled: true
```
