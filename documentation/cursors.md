---
id: cursors
title: Cursors
---

> **NOTE:** The cursors API is currently in beta.

Liftbridge partitions do not maintain any state on consumers such as tracking
where consumers are in the log. This reduces complexity on the server and makes
subscriptions more efficient. However, Liftbridge provides an API for storing
consumer partition cursors. A _cursor_ is simply an offset position into a log
which is uniquely identified by an ID. This allows consumers to checkpoint
their position in a partition such that they can resume processing later or
pick up where they left off in the event of a failure.

Cursor management is exposed through the Liftbridge [gRPC
API](https://github.com/liftbridge-io/liftbridge-api/blob/master/api.proto).
The `SetCursor` endpoint takes a `SetCursorRequest` which specifies the stream,
partition, ID, and offset for the cursor. On success, this will have durably
stored the partition cursor. `FetchCursor` is then used to retrieve an offset
for a given cursor.

This is a low-level API that is used to durably store a partition cursor. Users
must determine how often to checkpoint cursors. This is a balance between
optimizing for processing performance (frequent checkpointing will reduce
performane) and optimizing for reduced reprocessing of messages in the event of
a consumer failover (infrequent checkpointing will result in more messages
being reprocessed upon consumer recovery).

This low-level API will serve as a building block for higher-level consumer
functionality such as consumer groups. This will allow consumers to reliably
consume streams without having to manage cursors at all.
