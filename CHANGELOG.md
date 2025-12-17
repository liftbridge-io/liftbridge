# Changelog

## v26.01.1 (Unreleased)

### Breaking Changes
- Go version requirement: 1.21 -> 1.25.3

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

### Docker Updates
- Updated all Dockerfiles to Go 1.25-alpine
- Updated dev-cluster NATS image from 2.6.4 to 2.12.3
- Updated dev-cluster base image from debian:stretch-slim to debian:bookworm-slim
- Removed obsolete docker/circleci/ folder

### Raft v1.7.3 Compatibility
This release enables compatibility with hashicorp/raft v1.7.3, which includes:
- Pre-vote protocol (enabled by default)
- go-msgpack v2 upgrade
- CommitIndex API
- Various performance and stability improvements

### Maintainers
Project is now maintained by [Basekick Labs](https://github.com/basekick-labs),
creators of [Arc](https://github.com/basekick-labs/arc).
