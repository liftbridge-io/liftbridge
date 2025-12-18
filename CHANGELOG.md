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
