# Configuration

Liftbridge provides limited configuration through command-line flags and full
configuration using a configuration file. Flags will always take precedent
over settings in the configuration file.

The configuration file is passed in using the `--config` flag:

```
$ liftbridge --config liftbridge.conf
```

To get a full list of the CLI flags, use the `--help` flag:

```
$ liftbridge --help
NAME:
   liftbridge - Lightweight, fault-tolerant message streams

USAGE:
   liftbridge [global options] command [command options] [arguments...]

VERSION:
   0.0.1

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --config FILE, -c FILE                      load configuration from FILE
   --server-id value, --id value               ID of the server in the cluster if there is no stored ID (default: random ID)
   --namespace value, --ns value               cluster namespace (default: "liftbridge-default")
   --nats-servers ADDR[,ADDR], -n ADDR[,ADDR]  connect to NATS cluster at ADDR[,ADDR] (default: "nats://localhost:4222")
   --data-dir DIR, -d DIR                      store data in DIR (default: "/tmp/liftbridge/<namespace>")
   --port value, -p value                      port to bind to (default: 9292)
   --tls-cert value                            server certificate file
   --tls-key value                             private key for server certificate
   --level value, -l value                     logging level [debug|info|warn|error] (default: "info")
   --raft-bootstrap-seed                       bootstrap the Raft cluster by electing self as leader if there is no existing state
   --raft-bootstrap-peers value                bootstrap the Raft cluster with the provided list of peer IDs if there is no existing state
   --help, -h                                  show help
   --version, -v                               print the version
```

## Configuration File Format

The configuration file format borrows from the [NATS configuration
format](https://nats.io/documentation/server/gnatsd-config/). It is a flexible
format that combines the best of traditional configuration formats and newer
styles such as JSON and YAML.

The config file format supports the following syntax:

- Mixed Arrays: `[...]`
- Nested Maps: `{...}`
- Multiple comment types: `#` and `//`
- Key value assigments using:
  - Equals sign (`foo = 2`)
  - Colon (`foo: 2`)
  - Whitespace (`foo 2`)
- Maps can be assigned with no key separator
- Semicolons as value terminators in key/value assignments are optional

## Example Configuration File

An example configuration file is shown below.

```
listen: localhost:9293
data.dir: /tmp/liftbridge/server-2
log.level: debug

# Define NATS cluster to connect to.
nats {
    servers: ["nats://localhost:4300", "nats://localhost:4301"]
}

# Specify message log settings.
log {
    retention.max.age: "24h"
}

# Specify cluster settings.
clustering {
    server.id: server-2
    raft.logging: true
    raft.bootstrap.seed: true
    replica.max.lag.time: "20s"
}
```

## Configuration Settings

Below is the list of Liftbridge configuration settings, including the name of
the setting in the configuration file and the CLI flag if it exists.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| listen | | The server listen host/port. | string | 0:0:0:0:9292  | |
| host | | The server host. | string | localhost | |
| port | port | The server port. | int | 9292 | |
| tls.key | tls-key | The private key file for server certificate. This must be set in combination with `tls.cert` to enable TLS. | string | |
| tls.cert | tls-cert | The server certificate file. This must be set in combination with `tls.key` to enable TLS. | string | |
| log.level | level | The logging level. | string | info | [debug, info, warn, error] |
| log.recovery | | Log messages resulting from the replay of the Raft log on server recovery. | bool | false | |
| data.dir | data-dir | The directory to store data in. | string | /tmp/liftbridge/namespace | |
| batch.max.messages | | The maximum number of messages to batch when writing to disk. | int | 1024 |
| batch.wait.time | | The time to wait to batch more messages when writing to disk. | duration | 0 | |
| metadata.cache.max.age | | The maximum age of cached broker metadata. | duration | 2m | |
| nats | | NATS configuration. | map | | [See below](#nats-configuration-settings) |
| log | | Stream write-ahead log configuration. | map | | [See below](#log-configuration-settings) |
| clustering | | Broker cluster configuration. | map | | [See below](#cluster-configuration-settings) |

### NATS Configuration Settings

Below is the list of the configuration settings for the `nats` part of
the configuration file.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| servers | nats-servers | List of NATS hosts to connect to. | list | nats://localhost:4222 | |

### Log Configuration Settings

Below is the list of the configuration settings for the `log` part of
the configuration file.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| retention.max.bytes | | The maximum size a stream's log can grow to, in bytes, before we will discard old log segments to free up space. A value of 0 indicates no limit. | int64 | 0 | |
| retention.max.messages | | The maximum size a stream's log can grow to, in number of messages, before we will discard old log segments to free up space. A value of 0 indicates no limit. | int64 | 0 | |
| retention.max.age | | The TTL for stream log segment files, after which they are deleted. A value of 0 indicates no TTL. | duration | 168h | |
| cleaner.interval | | The frequency to check if a new stream log segment file should be rolled and whether any segments are eligible for deletion based on the retention policy or compaction if enabled. | duration | 5m | |
| log.roll.time | | The maximum time before a new stream log segment is rolled out. A value of 0 means new segments will only be rolled when `segment.max.bytes` is reached. Retention is always done a file at a time, so a larger value means fewer files but less granular control over retention. | duration | value of `retention.max.age` | |
| segment.max.bytes | | The maximum size of a single stream log segment file in bytes. Retention is always done a file at a time, so a larger segment size means fewer files but less granular control over retention. | int64 | 268435456 | |
| compact | | Enables stream log compaction. Compaction works by retaining only the latest message for each key and discarding older messages. The frequency in which compaction runs is controlled by `cleaner.interval`. | bool | false | |
| compact.max.goroutines | | The maximum number of concurrent goroutines to use for compaction on a stream log (only applicable if `compact` is enabled). | int | 10 | |

### Clustering Configuration Settings

Below is the list of the configuration settings for the `clustering` part of
the configuration file.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| server.id | server-id | ID of the server in the cluster. | string | random id | string with no spaces or periods |
| namespace | namespace | Cluster namespace. | string | liftbridge-default | string with no spaces or periods |
| raft.snapshot.retain | | The number Raft log snapshots to retain on disk. | int | 2 | |
| raft.snapshot.threshold | | Controls how many outstanding logs there must be before taking a snapshot. This prevents excessive snapshots when a small set of logs can be replayed. | int | 8192 | |
| raft.cache.size | | The number of Raft logs to hold in memory for quick lookup. | int | 512 | |
| raft.bootstrap.seed | raft-bootstrap-seed | Bootstrap the Raft cluster by electing self as leader if there is no existing state. | bool | false | |
| raft.bootstrap.peers | raft-bootstrap-peers | Bootstrap the Raft cluster with the provided list of peer IDs if there is no existing state. | list | | |
| raft.logging | | Enables logging in the Raft subsystem. | bool | false | |
| replica.max.lag.time | | If a follower hasn't sent any replication requests or hasn't caught up to the leader's log end offset for at least this time, the leader will remove the follower from ISR. | duration | 10s | |
| replica.max.leader.timeout | | If a leader hasn't sent any replication responses for at least this time, the follower will report the leader to the controller. If a majority of the replicas report the leader, a new leader is selected by the controller. | duration | 10s | |
| replica.fetch.timeout | | Timeout duration for follower replication requests. | duration | 3s | |
| min.insync.replicas | | Specifies the minimum number of replicas that must acknowledge a stream write before it can be committed. If the ISR drops below this size, messages cannot be committed. | int | 1 | [1,...] |
