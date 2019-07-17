# Replication Protocol

Liftbridge relies on Raft for metadata replication and leadership election.
However, for replication of streams, rather than using Raft or other
quorum-based techniques, we use a technique [similar to
Kafka](https://www.confluent.io/blog/hands-free-kafka-replication-a-lesson-in-operational-simplicity/).
For each stream, we maintain an in-sync replica set (ISR), which is all of the
replicas currently up to date with the leader (at stream creation time, this is
all of the replicas). During replication, the leader writes messages to a
write-ahead log, and we only wait on replicas in the ISR before committing. If
a replica falls behind or fails, it's removed from the ISR. If the leader
fails, any replica in the ISR can take its place. If a failed replica catches
back up, it rejoins the ISR. The general stream replication process is as
follows:

1. Client creates a stream with a `ReplicationFactor` of `n`.
1. Metadata leader selects `n` replicas to participate and one leader at random
   (this comprises the initial ISR).
1. Metadata leader replicates the stream via Raft to the entire cluster. This
   process also assigns a monotonically increasing `Epoch` to the stream as
   well as a `LeaderEpoch`. All replicas store a durable cache of each
   `LeaderEpoch` and the offset of the first message for the epoch used for
   recovery purposes described below.
1. The nodes participating in the stream initialize it, and the leader
   subscribes to the NATS subject.
1. The leader initializes the high watermark (HW) to -1. This is the offset of
   the last committed message in the stream (-1 meaning, initially, no
   messages are committed).
1. The leader begins sequencing messages from NATS and writes them to the log
   uncommitted.
1. Followers consume from the leader's log to replicate messages to their own
   log. We piggyback the leader's HW and `LeaderEpoch` on these responses.
   Replicas update their `LeaderEpoch` cache as needed and periodically
   checkpoint the HW to stable storage.
1. Replicas acknowledge they've replicated the message via replication requests
   to the leader.
1. Once the leader has heard from the ISR, the message is committed and the HW
   is updated.

Note that clients only see committed messages in the log.

## Failure Modes

There are a variety of failures that can occur in the replication process. A
few of them are described below along with how they are mitigated.

### Stream Leader Failure

If a follower suspects the leader has failed, it will notify the metadata
leader. If the metadata leader receives a notification from the majority of the
ISR within a bounded period, it will select a new leader for the stream, apply
this update to the Raft group, and notify the replica set. Leader reports
include the current stream `Epoch` and `LeaderEpoch` which is checked against
by the metadata leader.

Committed messages are always preserved during a leadership change, but
uncommitted messages could be lost. When a stream leader is changed, the
stream's `Epoch` is incremented.

### Metadata Leader Failure

If the metadata leader fails, Raft will handle electing a new leader. The
metadata Raft group stores the leader and ISR for every stream, so failover of
the metadata leader is not a problem.

### Stream Follower Failure

If the stream leader detects that a follower has failed or fallen too far
behind, it removes the replica from the ISR by notifying the metadata leader.
This results in incrementing the stream's `Epoch`. The metadata leader
replicates this fact via Raft. The stream leader continues to commit messages
with fewer replicas in the ISR, entering an under-replicated state.

When a failed follower is restarted, it requests the last offset for the
current `LeaderEpoch` from the stream leader. It then truncates its log up to
this offset, which removes any potentially uncommitted messages from the log.
The replica then begins fetching messages from the leader starting at this
offset. Once the replica has caught up, it's added back into the ISR and the
system resumes its fully replicated state. Adding a replica back into the
ISR results in incrementing the stream's `Epoch`.

We truncate uncommitted messages from the log using the `LeaderEpoch` rather
than the HW because there are certain edge cases that can result in data loss
or divergent log lineages with the latter. This has been [described in detail](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation#KIP-101-AlterReplicationProtocoltouseLeaderEpochratherthanHighWatermarkforTruncation-Solution)
by the maintainers of Kafka.
