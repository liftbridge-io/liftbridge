---
id: version-v1.8.0-cursors
title: Cursors
original_id: cursors
---

> **NOTE:** The cursors API is currently in beta. This includes the `SetCursor`
> and `FetchCursor` endpoints.

Liftbridge partitions do not maintain any state on consumers such as tracking
where consumers are in the log. This reduces complexity on the server and makes
subscriptions more efficient. However, Liftbridge provides an API for storing
consumer partition cursors. A _cursor_ is simply an offset position into a log
which is uniquely identified by an ID. This allows consumers to checkpoint
their position in a partition such that they can resume processing later or
pick up where they left off in the event of a failure.

> **Use Case Note**
>
> For most cases, the cursors API should not generally be used due to its
> low-level nature. Instead, [consumer groups](./consumer_groups.md) provide
> higher-level functionality including transparent consumer offset tracking.

Cursor management is exposed through the Liftbridge [gRPC
API](https://github.com/liftbridge-io/liftbridge-api/blob/master/api.proto).
The `SetCursor` endpoint takes a `SetCursorRequest` which specifies the
identifier, stream, partition, and offset for the cursor. On success, this will
have durably stored the partition cursor. `FetchCursor` is then used to
retrieve an offset for a given cursor.

An example of this is shown below, in which a cursor is retrieved to resume the
subscription and is subsequently updated each time a message is processed.

```go
var (
	ctx      = context.Background()
	cursorID = "mycursor"
    stream   = "foo"
)
// Retrieve offset to resume processing from.
offset, err := client.FetchCursor(ctx, cursorID, stream, 0)
if err != nil {
	panic(err)
}
if err := client.Subscribe(ctx, stream, func(msg *lift.Message, err error) {
	if err != nil {
		panic(err)
	}
	fmt.Println(msg.Timestamp(), msg.Offset(), string(msg.Key()), string(msg.Value()))
    // Checkpoint current position.
	if err := client.SetCursor(ctx, cursorID, stream, msg.Partition(), msg.Offset()); err != nil {
		panic(err)
	}
}, lift.StartAtOffset(offset+1)); err != nil {
	panic(err)
}
<-ctx.Done()
```

This is a low-level API that is used to durably store a partition cursor. Users
must determine how often to checkpoint cursors. This is a balance between
optimizing for processing performance (frequent checkpointing will reduce
performance) and optimizing for reprocessing of messages in the event of a
consumer failover (infrequent checkpointing will result in more messages being
reprocessed upon consumer recovery).

This low-level API serves as a building block for higher-level consumer
functionality such as [consumer groups](./consumer_groups.md). Groups allow
consumers to reliably consume streams without having to manage cursors at all.

## Configuring Cursor Management

Cursors are stored in an internal Liftbridge stream named `__cursors`. This
stream is not intended to be accessed by consumers directly. The stream is
partitioned by cursor key. Keys are of the following form:

```plaintext
<cursorID>,<stream>,<partition>
```

This means that cursors for the same ID are stored in the same partition. The
`__cursors` stream is also compacted by key such that only the most recent
versions of cursors are retained. Due to this partitioning, clients must send
`SetCursor` and `FetchCursor` requests to the respective partition leader. This
mapping should be implemented by client libraries.

Configuration settings for cursor management are grouped under the `cursors`
namespace. See the full list of configuration options
[here](./configuration.md#cursors-configuration-settings).

By default, the internal `__cursors` stream is disabled. To enable it, you must
set the number of partitions to something greater than 0:

```yaml
cursors.stream.partitions: 10
```

Additionally, partitions in the `__cursors` stream automatically pause when
they go idle (i.e. do not receive a cursor update or fetch) for 1 minute by
default. This can be changed or disabled with the
`cursors.stream.auto.pause.time` setting:

```yaml
cursors.stream.auto.pause.time: 0
```
