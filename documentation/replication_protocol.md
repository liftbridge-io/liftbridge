---
id: replication-protocol
title: Replication Protocol
---

Liftbridge relies on Raft for metadata replication and leadership election.
However, for replication of streams, rather than using Raft or other
quorum-based techniques, we use a technique [similar to
Kafka](https://www.confluent.io/blog/hands-free-kafka-replication-a-lesson-in-operational-simplicity/).
For each stream partition, we maintain an in-sync replica set (ISR), which is all
of the replicas currently up to date with the leader (at partition creation time,
this is all of the replicas). During replication, the leader writes messages to a
write-ahead log, and we only wait on replicas in the ISR before committing. If
a replica falls behind or fails, it's removed from the ISR. If the leader
fails, any replica in the ISR can take its place. If a failed replica catches
back up, it rejoins the ISR. The general partition replication process is as
follows:

1. Client creates a partition with a `ReplicationFactor` of `n`.
1. Metadata leader selects `n` replicas to participate and one leader at random
   (this comprises the initial ISR).
1. Metadata leader replicates the partition via Raft to the entire cluster. This
   process also assigns a monotonically increasing `Epoch` to the partition as
   well as a `LeaderEpoch`. All replicas store a durable cache of each
   `LeaderEpoch` and the offset of the first message for the epoch used for
   recovery purposes described below.
1. The nodes participating in the partition initialize it, and the leader
   subscribes to the NATS subject.
1. The leader initializes the high watermark (`HW`) to -1. This is the offset of
   the last committed message in the partition (-1 meaning, initially, no
   messages are committed).
1. The leader begins sequencing messages from NATS and writes them to the log
   uncommitted.
1. Followers consume from the leader's log to replicate messages to their own
   log. We piggyback the leader's `HW` and `LeaderEpoch` on these responses.
   Replicas update their `LeaderEpoch` cache as needed and periodically
   checkpoint the `HW` to stable storage. If the `LeaderEpoch` on the response
   does not match the expected `LeaderEpoch`, the follower drops the response.
   The same is true if the offset of the first message in the response is less
   than the expected next offset. The follower uses the leader's `HW` to update
   their own `HW`.
1. Followers acknowledge they've replicated the message via replication
   requests to the leader.
1. Once the leader has heard from the ISR, the message is committed and the
   `HW` is updated.

Note that clients only see committed messages in the log.

When a follower has caught up with the leader's log (i.e. it reaches the
leader's log end offset, or LEO), the leader will register a _data waiter_ for
the follower on the log. This waiter is used to signal when new messages have
been written to the log. Upon catching up, followers will receive an empty
replication response from the leader and sleep for a duration
`replica.max.idle.wait` to avoid overloading the leader with replication
requests. After sleeping, they will make subsequent replication requests. Even
if data is not available, this will ensure timely health checks occur to avoid
spurious ISR shrinking. If new messages are written while a follower is idle,
the data waiter is signalled which causes the leader to send a notification to
the follower to preempt the sleep and begin replicating again.

## Failure Modes

There are a variety of failures that can occur in the replication process. A
few of them are described below along with how they are mitigated.

### Partition Leader Failure

If a follower suspects the leader has failed, it will notify the metadata
leader. If the metadata leader receives a notification from the majority of the
ISR within a bounded period, it will select a new leader for the partition, apply
this update to the Raft group, and notify the replica set. Leader reports
include the current partition `Epoch` and `LeaderEpoch` which is checked against
by the metadata leader.

Committed messages are always preserved during a leadership change, but
uncommitted messages could be lost. When a partition leader is changed, the
partition's `Epoch` is incremented.

### Metadata Leader Failure

If the metadata leader fails, Raft will handle electing a new leader. The
metadata Raft group stores the leader and ISR for every stream, so failover of
the metadata leader is not a problem.

### Partition Follower Failure

If the partition leader detects that a follower has failed or fallen too far
behind, it removes the replica from the ISR by notifying the metadata leader.
This results in incrementing the partition's `Epoch`. The metadata leader
replicates this fact via Raft. The partition leader continues to commit messages
with fewer replicas in the ISR, entering an under-replicated state.

When a failed follower is restarted, it requests the last offset for the
current `LeaderEpoch` from the partition leader, called a `LeaderEpoch` offset
request. It then truncates its log up to this offset, which removes any
potentially uncommitted messages from the log.  The replica then begins
fetching messages from the leader starting at this offset. Once the replica has
caught up, it's added back into the ISR and the system resumes its fully
replicated state. Adding a replica back into the ISR results in incrementing
the partition's `Epoch`.

We truncate uncommitted messages from the log using the `LeaderEpoch` rather
than the HW because there are certain edge cases that can result in data loss
or divergent log lineages with the latter. This has been [described in
detail](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation)
by the maintainers of Kafka.

## Replication RPC Protocol

Replication RPCs are made over internal NATS subjects. Replication requests for
a partition are sent to `<namespace>.<stream>.<partition>.replicate` which is a
subject the partition leader subscribes to. The request data is a
[protobuf](https://github.com/liftbridge-io/liftbridge/blob/8bee0478da97711dc2a8e1fdae8b2d2e3086c756/server/proto/internal.proto#L87-L90)
containing the ID of the follower and the offset they want to begin fetching
from. The NATS message also includes a random [reply
inbox](https://nats-io.github.io/docs/developer/sending/replyto.html) the
leader uses to send the response to.

The leader response uses a binary format consisting of the following:

```
   0     1     2     3     4     5     6     7     8     9     10    11    12    13    14    15
+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+...
|                  LeaderEpoch                  |                       HW                      |...
+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+...
|<-------------------8 bytes------------------->|<-------------------8 bytes------------------->|
```

The remainder of the response consists of message data. If the follower is
caught up, there won't be any message data. The `LeaderEpoch` and `HW` are
always present, so there are 16 bytes guaranteed in the response.

The `LeaderEpoch` offset requests also use internal NATS subjects similar to
replication RPCs. These requests are sent to
`<namespace>.<stream>.<partition>.offset`. The request and response both use a
[protobuf](https://github.com/liftbridge-io/liftbridge/blob/8bee0478da97711dc2a8e1fdae8b2d2e3086c756/server/proto/internal.proto#L92-L98)
containing the `LeaderEpoch` and offset, respectively. Like replication,
responses are sent to a random reply subject included on the NATS request
message.

Notifications of new data are sent on the NATS subject
`<namespace>.notify.<serverID>`. This is also a protobuf containing the stream
name and ID of the partition with new data available. Upon receiving this
notification, the follower preempts the replication loop for the respective
partition, if idle, to begin replicating.
