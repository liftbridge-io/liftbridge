# Changelog

## Versioning Convention

Starting with this release, Liftbridge uses **CalVer** (Calendar Versioning) in the format `YY.MM.PATCH`:
- `YY` - Two-digit year (e.g., 26 for 2026)
- `MM` - Month (1-12)
- `PATCH` - Patch number within the month

This replaces the previous semantic versioning (v1.x.x) to better reflect the project's release cadence.

---

## v26.01.1 (Unreleased)

### Breaking Changes
- **Versioning**: Changed from semantic versioning (v1.x.x) to CalVer (YY.MM.PATCH)
- **Go version**: Minimum requirement changed from 1.21 to 1.25.3
- **Version variable**: `server.Version` changed from `const` to `var` for build-time injection

### New Features

#### Reverse Subscription Support ([#290](https://github.com/liftbridge-io/liftbridge/issues/290))
Added the ability to subscribe to streams in reverse order (newest to oldest messages).

**Use Cases**:
- **Consumer cursor optimization**: Finding the latest cursor position is now O(k) instead of O(n), where k is the number of updates per cursor vs n total messages
- **Recent events display**: UIs can show the most recent messages first without scanning the entire stream
- **Debugging**: Quickly examine recent messages when troubleshooting issues
- **Event search**: Find recent occurrences of specific events by reading backwards

**API Changes**:
- Added `Reverse` boolean field to `SubscribeRequest` protobuf message
- When `Reverse=true`, messages are delivered from newest to oldest
- Works with all start positions (`LATEST`, `EARLIEST`, `OFFSET`, `TIMESTAMP`)
- Compatible with both committed and uncommitted reads

**Implementation**:
- Added `ReverseReader` and `MessageReader` interface in commitlog package
- Added `reverseIndexScanner` for backward index traversal
- Added `reverseSegmentScanner` for backward segment reading
- Optimized `getLatestCursorOffset()` to use reverse subscription internally

**Example** (using go-liftbridge):
```go
// Subscribe in reverse order starting from the latest message
client.Subscribe(ctx, "my-stream", func(msg *lift.Message, err error) {
    fmt.Println(msg.Offset(), string(msg.Value()))
}, lift.StartAtLatestReceived(), lift.Reverse())
```

#### GitHub Actions Release Workflow
Added automated release workflow triggered by `release/*` branches:
- **Multi-platform binaries**: darwin/amd64, darwin/arm64, linux/amd64, linux/arm64, windows/amd64, windows/arm64
- **Linux packages**: Debian (.deb) and RPM (.rpm) for amd64/arm64
- **Docker images**: Multi-arch images pushed to `ghcr.io/liftbridge-io/liftbridge`
- **Automated testing**: Runs full test suite, binary verification, and Docker image tests before release
- **Draft releases**: Creates GitHub draft release with all artifacts and checksums

#### Package Installation
Linux packages now include:
- Systemd service file (`/lib/systemd/system/liftbridge.service`)
- Default configuration (`/etc/liftbridge/liftbridge.yaml`)
- Data directory (`/var/lib/liftbridge/data`)
- Dedicated `liftbridge` user/group

#### Anonymous Telemetry
Added opt-out telemetry to help improve Liftbridge by collecting anonymous usage statistics.

**What's Collected**:
- Instance ID (random UUID, persistent per installation)
- Liftbridge version
- OS information (name, version, architecture)
- CPU cores (physical/logical)
- Total system memory

**Privacy**:
- No message data, stream names, or personally identifiable information
- No network addresses, credentials, or performance metrics
- Data sent to `telemetry.basekick.net` every 24 hours
- Easy opt-out via configuration or environment variable

**Configuration**:
```yaml
telemetry:
  enabled: false  # Set to false to disable
  interval:
    seconds: 86400  # Default: 24 hours
```

Or via environment variables:
```bash
export LIFTBRIDGE_TELEMETRY_ENABLED=false
```

See the [Telemetry Documentation](https://docs.basekick.net/liftbridge/operations/telemetry) for full details.

### Dependencies Updated
| Package | Before | After |
|---------|--------|-------|
| `github.com/hashicorp/raft` | v1.1.2 | v1.7.3 |
| `github.com/hashicorp/go-hclog` | v0.14.1 | v1.6.2 |
| `github.com/liftbridge-io/nats-on-a-log` | v0.0.0-20200818 | v0.0.0-20251217 |

### Code Changes
- Fixed raft bootstrap with `RaftMaxQuorumSize`: ensure local server is always
  a voter to support raft v1.7.3's pre-vote protocol requirement
- Migrated CI from CircleCI to GitHub Actions
- Updated `server/version.go` to support build-time version injection via ldflags

### Docker Updates
- Updated all Dockerfiles to Go 1.25-alpine
- Updated dev-cluster NATS image from 2.6.4 to 2.12.3
- Updated dev-cluster base image from debian:stretch-slim to debian:bookworm-slim
- Added VERSION build argument for proper version tagging
- Optimized Dockerfile layer caching
- Removed obsolete docker/circleci/ folder

### Bug Fixes

#### Segment Deletion Edge Case
Fixed a bug where partial deletion failure could leave the system in an inconsistent state.

**Problem**: If segment file deletion failed partway through (e.g., due to permissions or I/O error), some segments would be deleted while others remained, but an error was returned. This could cause data inconsistency.

**Solution**: Implemented a mark-then-delete approach:
1. All segments are first marked as deleted (atomic per-segment), removing them from the read path
2. File deletion is then attempted for each segment
3. If file deletion fails, segments are already marked deleted and won't be visible to readers
4. Remaining files will be cleaned up on the next cleanup cycle

**Changes**:
- Added `deleted` flag to segment struct with `MarkDeleted()` and `IsDeleted()` methods
- Added `deleteSegments()` helper to `deleteCleaner` implementing the two-phase approach
- Refactored `applyMessagesLimit`, `applyBytesLimit`, and `applyAgeLimit` to use the new helper

#### Corrupt Index File Recovery ([#411](https://github.com/liftbridge-io/liftbridge/issues/411))
Fixed a startup panic when index files become corrupted.

**Problem**: If an index file became corrupted (e.g., due to unclean shutdown, disk errors), Liftbridge would panic on startup with "corrupt index file" error and fail to start. This left users unable to recover without manual intervention.

**Solution**: Implemented automatic index rebuild from the log file:
1. When corruption is detected during index initialization, the corrupt index is deleted
2. A new index is created and rebuilt by scanning the log file
3. All valid message entries are re-indexed from the log data
4. Server startup proceeds normally with the rebuilt index

**Changes**:
- Added `rebuildIndex()` method to segment that scans log file and recreates index entries
- Modified `setupIndex()` to catch `errIndexCorrupt` and attempt automatic recovery
- Added tests for corrupt index detection and recovery scenarios

#### Snapshot Restore Panic ([#414](https://github.com/liftbridge-io/liftbridge/issues/414))
Fixed a startup panic when restoring from a Raft snapshot.

**Problem**: When a node started and restored state from a Raft snapshot, it would panic with a nil pointer dereference at `partition.go:1311`. This occurred because partitions were trying to start leader/follower loops before the API server was initialized.

**Solution**: Mark streams and consumer groups as "recovered" during snapshot restore, deferring their startup until `finishedRecovery()` is called after log replay completes. This ensures `s.api` is initialized before partitions attempt to start.

**Changes**:
- Modified `Restore()` in fsm.go to pass `recovered=true` to `applyCreateStream()` and `applyCreateConsumerGroup()`

#### Signal Handling Race with Embedded NATS ([#373](https://github.com/liftbridge-io/liftbridge/issues/373))
Fixed a race condition when using embedded NATS that could prevent graceful shutdown.

**Problem**: When Liftbridge runs with embedded NATS (`EmbeddedNATS: true`), both servers registered their own signal handlers for SIGINT/SIGTERM. Whichever handler ran first would win - if NATS won, it would call `os.Exit()` immediately, preventing Liftbridge from performing graceful shutdown (closing partitions, draining NATS connections, stopping Raft).

**Solution**: Set `opts.NoSigs = true` when creating the embedded NATS server, disabling NATS's signal handling and allowing Liftbridge to handle all signals for proper graceful shutdown.

**Changes**:
- Modified `startEmbeddedNATS()` in server.go to set `opts.NoSigs = true`

#### Leader Not In ISR Panic ([#354](https://github.com/liftbridge-io/liftbridge/issues/354))
Fixed a nil pointer dereference when a partition leader is not in the ISR.

**Problem**: When restoring from a Raft snapshot, if the snapshot contained inconsistent state where a partition's leader was not in its ISR (In-Sync Replicas), the server would panic with a nil pointer dereference at `partition.go:812`. This could happen if a node was removed from the ISR via ShrinkISR but a snapshot was taken before a new leader election occurred.

**Solution**: Added a defensive nil check in `becomeLeader()`. If the leader is not found in the ISR, it logs a warning and adds itself to the ISR with the current offset, allowing the server to recover from the corrupt state.

**Changes**:
- Modified `becomeLeader()` in partition.go to check if the server is in the ISR before accessing it
- Added auto-recovery logic to add self to ISR if missing
- Added test case `TestPartitionBecomeLeaderNotInISR` to verify the fix

### Raft v1.7.3 Compatibility
This release enables compatibility with hashicorp/raft v1.7.3, which includes:
- Pre-vote protocol (enabled by default)
- go-msgpack v2 upgrade
- CommitIndex API
- Various performance and stability improvements

### Maintainers
Project is now maintained by [Basekick Labs](https://github.com/basekick-labs),
creators of [Arc](https://github.com/basekick-labs/arc).

---

## Previous Releases

For releases prior to v26.01.1, see the [GitHub Releases](https://github.com/liftbridge-io/liftbridge/releases) page.
