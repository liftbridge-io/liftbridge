---
id: roadmap
title: Product Roadmap
---

This roadmap is not intended to provide a firm timeline or commitment of
features and capabilities, nor does it include the comprehensive set of planned
work. Its purpose is to offer visibility into the major items that are planned
and the rough sequencing of them. It also is intended to lay out the vision and
direction of the project. Items not included in the roadmap may still be
implemented. Similarly, items in the roadmap may end up not being implemented
or may look different than as described here. As such, the roadmap exists as a
living document and will be updated as things evolve.

Please [create an issue](https://github.com/liftbridge-io/liftbridge/issues/new)
to provide input on the roadmap or refer to [existing issues](https://github.com/liftbridge-io/liftbridge/issues)
to comment on particular roadmap items.

---

## Completed Features

The following features have been implemented and released:

### Consumer Groups ([#46](https://github.com/liftbridge-io/liftbridge/issues/46))

High-level consumer functionality with durable subscriptions, balanced stream
consumption, and fault-tolerant consumers with automatic rebalancing.

### Authentication and Authorization ([#36](https://github.com/liftbridge-io/liftbridge/issues/36))

mTLS client authentication and Casbin-based ACL authorization with granular
access control for streams and partitions.

### Encryption at Rest

AES-256-GCM encryption for log segments with key management support.

### Log Compaction

Key-based log compaction for changelog/table-style streams.

---

## Phase 1: Foundation (v26.02)

### Message Compression

**Status**: Planned

Reduce storage costs and network bandwidth by 60-80% with support for multiple
compression codecs.

**Implementation**:
- Compression codec field in message header (gzip, snappy, lz4, zstd)
- Compress on write, decompress on read
- Per-stream configuration option
- Batch compression for efficiency

### Monitoring API / Prometheus Metrics ([#222](https://github.com/liftbridge-io/liftbridge/issues/222))

**Status**: Planned

Expose monitoring information and metrics via Prometheus endpoint for
production monitoring and alerting.

**Metrics to expose**:
- `liftbridge_messages_in_total` (counter, per stream/partition)
- `liftbridge_messages_out_total` (counter)
- `liftbridge_bytes_in_total`, `liftbridge_bytes_out_total`
- `liftbridge_partition_lag` (gauge)
- `liftbridge_isr_count` (gauge)
- `liftbridge_replication_lag_ms` (histogram)
- `liftbridge_consumer_group_members` (gauge)
- `liftbridge_request_latency_ms` (histogram, per API)

### Admin CLI Tool

**Status**: Planned

Command-line interface for easier cluster management without writing code.

**Commands**:
```bash
liftbridge admin streams list
liftbridge admin streams create --name foo --partitions 3
liftbridge admin streams delete --name foo
liftbridge admin groups list
liftbridge admin cluster status
```

### Bug Fixes: Deletion Edge Case

**Status**: Done (v26.01.1)

Fixed partial deletion failure that could leave system in inconsistent state.
Implemented mark-then-delete approach: segments are marked deleted first, then files are removed.

### Bug Fixes: Corrupt Index Recovery ([#411](https://github.com/liftbridge-io/liftbridge/issues/411))

**Status**: Done (v26.01.1)

Fixed startup panic when index files become corrupted. Server now automatically
rebuilds corrupt indexes from the log file.

### Bug Fixes: Snapshot Restore Panic ([#414](https://github.com/liftbridge-io/liftbridge/issues/414))

**Status**: Done (v26.01.1)

Fixed startup panic when restoring from a Raft snapshot. Partitions now defer
starting leader/follower loops until after recovery completes.

### Bug Fixes: Signal Handling Race ([#373](https://github.com/liftbridge-io/liftbridge/issues/373))

**Status**: Done (v26.01.1)

Fixed race condition when using embedded NATS that could prevent graceful shutdown.
Set `opts.NoSigs = true` to disable NATS's signal handling.

### Bug Fixes: Leader Not In ISR Panic ([#354](https://github.com/liftbridge-io/liftbridge/issues/354))

**Status**: Done (v26.01.1)

Fixed nil pointer dereference when partition leader is not in ISR during snapshot restore.
Added defensive check in `becomeLeader()` to add self to ISR if missing, allowing recovery
from corrupt snapshots.

### Bug Fixes: Slice Bounds Panic During Compaction ([#321](https://github.com/liftbridge-io/liftbridge/issues/321))

**Status**: Planned

Fix slice bounds out of range panic when compaction encounters corrupted messages.
Add bounds validation to `SerializedMessage.Key()`, `Value()`, and `Headers()` methods
in `server/commitlog/message.go` to gracefully handle corrupt data instead of panicking.

---

## Phase 2: Enterprise Features (v26.03)

### Multi-Format Ingestion (HTTP API)

**Status**: Planned

HTTP API with support for multiple data formats beyond gRPC/Protobuf.

**Supported Formats**:
- CSV (`text/csv`, `application/csv`)
- InfluxDB Line Protocol (`application/x-influxdb-line-protocol`)
- MessagePack Row (`application/msgpack`)
- MessagePack Columnar (`application/x-msgpack-columnar`)

**Endpoints**:
```
POST /v1/streams/{stream}/publish              # Auto-detect format
POST /v1/streams/{stream}/publish/csv          # Explicit CSV
POST /v1/streams/{stream}/publish/lineprotocol # Line Protocol
POST /v1/streams/{stream}/publish/msgpack      # MessagePack row
POST /v1/streams/{stream}/publish/msgpack-columnar
GET /health, GET /ready                        # Health checks
```

**Request Headers**:
- `Content-Type` - Format detection
- `X-Liftbridge-Key-Field` - Field to use as message key
- `X-Liftbridge-Ack-Policy` - none|leader|all

**Architecture**:
- New `server/ingest/` package for format parsers
- New `server/httpapi/` package for HTTP server
- HTTP runs on separate port (default 9293)
- All formats converted to Key/Value/Headers for storage
- Original format preserved in `X-Liftbridge-Format` header

**Format Mappings**:

CSV → Liftbridge:
```
timestamp,sensor_id,temp     →  Key: "sensor-1"
2024-01-15,sensor-1,23.5         Value: {"timestamp":"2024-01-15","sensor_id":"sensor-1","temp":23.5}
```

Line Protocol → Liftbridge:
```
weather,loc=us temp=82 1465839830  →  Key: "weather"
                                       Value: {"measurement":"weather","tags":{"loc":"us"},"fields":{"temp":82}}
```

MessagePack Columnar → Liftbridge (expanded to rows):
```
{"columns":{"id":[1,2],"name":["A","B"]}}  →  2 messages: {id:1,name:"A"}, {id:2,name:"B"}
```

### SASL Authentication

**Status**: Planned

Username/password authentication to complement mTLS for simpler deployments.

**Support**:
- SASL/PLAIN (username/password)
- SASL/SCRAM-SHA-256/512
- Pluggable user store (file-based, LDAP, DB)

### Authorization Enhancements ([#409](https://github.com/liftbridge-io/liftbridge/issues/409))

**Status**: Planned

Implement syncing of authorization policies across cluster nodes via Raft FSM.
This improves operator experience by not requiring policy files to be modified
across all nodes in a cluster and prevents permissions drift.

### Audit Logging

**Status**: Planned

Comprehensive audit logging for compliance and security investigation.

**Log events**:
- All authorization decisions
- Stream create/delete operations
- Authentication failures
- Admin actions

---

## Phase 3: Scale & Performance (v26.04+)

### Tiered Storage ([#110](https://github.com/liftbridge-io/liftbridge/issues/110))

**Status**: Planned

Transparent offloading of log segments to object storage (S3, GCS) for
cost-effective long-term retention and virtually infinite storage.

**Implementation**:
- Keep recent segments locally (hot tier)
- Upload sealed segments to S3/GCS (cold tier)
- Lazy fetch on read for cold segments
- Configurable retention per tier

```yaml
storage:
  tiered:
    enabled: true
    local.retention.hours: 24
    remote:
      type: s3
      bucket: liftbridge-archive
      region: us-east-1
```

### Idempotent Producer

**Status**: Planned

Prevent duplicates on retries, foundation for exactly-once semantics.

**Implementation**:
- Producer ID + sequence number per partition
- Server-side deduplication window
- Reject duplicates with specific error code

### Rate Limiting / Quotas

**Status**: Planned

Protect cluster from misbehaving clients with configurable limits.

**Quotas**:
- Produce bytes/sec per client
- Fetch bytes/sec per client
- Request rate per client
- Connection limits

### Distributed Tracing (OpenTelemetry)

**Status**: Planned

End-to-end visibility in microservices architectures.

**Features**:
- Trace context propagation via message headers
- Span creation for publish/subscribe operations
- Integration with Jaeger, Zipkin, etc.

### Progressive Shutdown / Graceful Shedding ([#317](https://github.com/liftbridge-io/liftbridge/issues/317))

**Status**: Planned

Progressive shutdown for rolling upgrades and cluster maintenance without service
disruption.

**Implementation**:
- Transfer partition leadership gradually via ChangeLeaderOp
- Remove self from ISR for follower partitions via ShrinkISROp
- Reject new client requests during shutdown phase
- Transfer metadata leadership if current node is metadata leader
- Configurable shutdown timeout and batch sizes

**Configuration**:
```yaml
shutdown:
  graceful: true
  timeout: 30s
  leadership.transfer.batch: 10
  leadership.transfer.delay: 100ms
```

**Complexity Notes**:
- Requires changes across server.go, api.go, partition.go, metadata.go
- Timing/coordination between leadership transfers to avoid Raft flood
- Must handle partial failures gracefully
- Extensive multi-node cluster testing required

---

## Phase 4: Advanced Features (Future)

### Transactions (Atomic Multi-Partition Writes)

**Status**: Future
**Depends on**: Idempotent Producer

Exactly-once semantics across partitions with two-phase commit protocol.

**Implementation**:
- Transaction coordinator
- Transaction log for recovery
- Isolation levels (read_committed, read_uncommitted)

### Federated Clustering ([#219](https://github.com/liftbridge-io/liftbridge/issues/219))

**Status**: Future

Federated Liftbridge clustering for geo-replication with async replication
between clusters, conflict resolution strategies, and active-passive/active-active modes.

### Optional NATS API ([#221](https://github.com/liftbridge-io/liftbridge/issues/221))

**Status**: Future

Allow the NATS API to be exposed optionally, turning NATS into an
implementation detail for users who only want the Liftbridge API.

### Kafka Bridge Connector ([#220](https://github.com/liftbridge-io/liftbridge/issues/220))

**Status**: Future

Bridge process that maps Kafka topics to Liftbridge streams to support
migration from Kafka or hybrid scenarios.

### Schema Registry Integration

**Status**: Future

Optional schema validation on publish with Avro/Protobuf/JSON Schema support.
Integration with Confluent Schema Registry or standalone registry.

### Flatbuffers and Zero-Copy Support ([#87](https://github.com/liftbridge-io/liftbridge/issues/87), [#185](https://github.com/liftbridge-io/liftbridge/issues/185))

**Status**: Future

Opt-in support for Flatbuffers and a zero-copy API for high-performance use cases.

### WebSocket API

**Status**: Future

Real-time subscriptions from browsers with JSON message format option.

### Dead Letter Queues

**Status**: Future

Automatic routing of failed messages with configurable retry policies.

---

## Known Issues / TODOs

| File | Issue | Priority |
|------|-------|----------|
| `delete_cleaner.go` | Partial deletion failure can leave inconsistent state | High |
| `compact_cleaner.go` | No configurable compaction lag | Medium |
| `compact_cleaner.go` | Small segments not merged after compaction | Medium |
| `commitlog.go` | HW flush to disk optimization needed | Medium |
| `groups.go` | Rebalancing optimization needed | Low |

---

## Comparison with Kafka

| Feature | Kafka | Liftbridge | Status |
|---------|-------|------------|--------|
| Compression | gzip, snappy, lz4, zstd | - | Phase 1 |
| Transactions | Yes | - | Phase 4 |
| Exactly-once | Yes | At-least-once | Phase 3+ |
| Tiered Storage | Yes | - | Phase 3 |
| Prometheus Metrics | Yes | - | Phase 1 |
| HTTP/REST API | Via REST Proxy | - | Phase 2 |
| Multi-Format Ingestion | Via Connect | - | Phase 2 |
| Schema Registry | Yes | - | Phase 4 |
| SASL Auth | Yes | mTLS only | Phase 2 |
| Quotas/Rate Limiting | Yes | - | Phase 3 |
| Consumer Groups | Yes | Yes | Done |
| Log Compaction | Yes | Yes | Done |
| Encryption at Rest | Yes | Yes | Done |

---

## Contributing

Contributions are welcome! If you're interested in working on any of these
features, please:

1. Check for existing issues or create a new one
2. Comment on the issue to express interest
3. Submit a PR with your implementation

For questions or discussion, open an issue or reach out to the maintainers.
