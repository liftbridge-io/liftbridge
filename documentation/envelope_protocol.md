---
id: envelope-protocol
title: Envelope Protocol
---

Liftbridge works by wrapping NATS with higher-level stream APIs. It is also
possible for a client to publish messages to Liftbridge via NATS directly.
Liftbridge accepts plain NATS messages, allowing it to make existing subjects
durable without any publisher changes. However, these messages will not have
features such as acks.

In order to opt into Liftbridge-specific features, the message must be prefixed
with an envelope header and be encoded as a `Message` (defined in
[api.proto](https://github.com/liftbridge-io/liftbridge-api/blob/master/api.proto)).
Stream acks sent over NATS use the same envelope protocol. This envelope
protocol is described below. Refer
[here](https://github.com/liftbridge-io/liftbridge-api) for more information on
the Liftbridge API.

## Liftbridge Envelope Header

All Liftbridge messages and RPCs sent over NATS are prefixed with an envelope
header. This includes client-facing messages, such as publishes and acks, as
well as internal RPCs like replication.

```plaintext
0               8               16              24              32
├───────────────┴───────────────┴───────────────┴───────────────┤
│                          Magic Number                         │
├───────────────┬───────────────┬───────────────┬───────────────┤
│    Version    │   HeaderLen   │     Flags     │    MsgType    │
├───────────────┴───────────────┴───────────────┴───────────────┤
│                       CRC-32C (optional)                      │
└───────────────────────────────────────────────────────────────┘
```

### Magic Number [4 bytes]

The Liftbridge magic number is `B9 0E 43 B4`. This was chosen by random but
deliberately restricted to invalid UTF-8 to reduce the chance of a collision.
This was also verified to not match known file signatures.

### Version [1 byte]

The version byte allows for future protocol upgrades. This should only be
bumped if the envelope format changes or if the message encoding changes in a
non-backwards-compatible way. Adding fields to the messages should not require
a version bump.

Currently, the only supported protocol version is v0, i.e. `0x00`.

### HeaderLen [1 byte]

The header length is the offset of the payload. This is included primarily for
safety.

### Flags [1 byte]

The flag bits are defined as follows:

| Bit | Description     |
| :-- | :-------------- |
| 0   | CRC-32C enabled |

### MsgType [1 byte]

This is the Liftbridge-specific message type the envelope contains:

| MsgType | Name                      | Description                                            | Internal |
| :------ | :------------------------ | :----------------------------------------------------- | :------- |
| 0       | Publish                   | Client-published message                               | no       |
| 1       | Ack                       | Server-published ack                                   | no       |
| 2       | ReplicationRequest        | Request to replicate partition data                    | yes      |
| 3       | ReplicationResponse       | Response to ReplicationRequest                         | yes      |
| 4       | RaftJoinRequest           | Request to join Raft cluster                           | yes      |
| 5       | RaftJoinResponse          | Response to RaftJoinRequest                            | yes      |
| 6       | LeaderEpochOffsetRequest  | Request for partition leader's latest offset for epoch | yes      |
| 7       | LeaderEpochOffsetResponse | Response to LeaderEpochOffsetRequest                   | yes      |
| 8       | PropagatedRequest         | Request forwarded to metadata leader                   | yes      |
| 9       | PropagatedResponse        | Response to PropagatedRequest                          | yes      |
| 10      | ServerInfoRequest         | Request for cluster information                        | yes      |
| 11      | ServerInfoResponse        | Response to ServerInfoRequest                          | yes      |
| 12      | PartitionStatusRequest    | Request to get partition status                        | yes      |
| 13      | PartitionStatusResponse   | Response to PartitionStatusRequest                     | yes      |
| 14      | PartitionNotification     | Signal new data is available for partition             | yes      |

### CRC-32C [4 bytes, optional]

The CRC-32C (Castagnoli) is the checksum of the payload (i.e. from HeaderLen to
the end). This is optional but should significantly reduce the chance that a
random NATS message is interpreted as a Liftbridge message.
