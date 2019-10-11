---
id: ha-and-consistency-configuration
title: Configuring for High Availability and Consistency
---

Liftbridge provides several parameters, outlined below, for controlling high
availability and data consistency (guaranteed delivery). It should be pointed
out that these characteristics are often at odds with each other.

## Replication Factor

The replication factor of a stream controls the number of nodes the stream's
partitions should be replicated to for redundancy. This value is set by the
client when creating a stream. The default replication factor is 1.

For high availability and durability of data, it is recommended to use a
replication factor of at least 3.

```go
// Create a stream with a replication factor of 3.
if err := client.CreateStream(context.Background(), "foo", "foo-stream", lift.ReplicationFactor(3)); err != nil {
    if err != lift.ErrStreamExists {
        panic(err)
    }
}
```

## Ack Policy

When publishing a message to Liftbridge, you can choose how many replicas must
store a message before it is acked. The Ack Policy controls this behavior and
is configured on each message. Valid values are:

- `AckPolicy_LEADER`: the ack is sent once the leader has written the message
  to its log
- `AckPolicy_ALL`: the ack is sent after the ISR replicas have written the
  message to their logs (i.e. the message is committed).
- `AckPolicy_NONE`: no ack is sent

The default value is `AckPolicy_LEADER`. `AckPolicy_ALL` provides the highest
consistency guarantee at the expense of slower writes.

## Minimum In-Sync Replica Set

You can set the minimum number of in-sync replicas (ISR) that must acknowledge
a stream write before it can be committed. If the ISR drops below this size,
messages cannot be committed. This is controlled with the `min.insync.replicas`
setting in `clustering` configuration.

By default, this value is 1, favoring availability over consistency. This
setting can be used in conjunction with the replication factor and ack policy
to enforce a quorum on writes which provides greater durability guarantees. For
example, for a stream with a replication factor of 3, `AckPolicy_ALL` and a
`min.insync.replicas` value of 2 will guarantee the message is written to at
least 2 replicas.

```plaintext
clustering {
    min.insync.replicas: 2
}
```
