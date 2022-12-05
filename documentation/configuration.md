---
id: configuration
title: Configuration
---

Liftbridge provides limited configuration through command-line flags and full
configuration using a configuration file. Flags will always take precedent
over settings in the configuration file.

The configuration file is passed in using the `--config` flag:

```shell
$ liftbridge --config liftbridge.yaml
```

To get a full list of the CLI flags, use the `--help` flag:

```shell
$ liftbridge --help
NAME:
   liftbridge - Lightweight, fault-tolerant message streams

USAGE:
   liftbridge [global options] command [command options] [arguments...]

VERSION:
   v1.9.0

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --config FILE, -c FILE                      load configuration from FILE
   --server-id value, --id value               ID of the server in the cluster if there is no stored ID (default: random ID)
   --namespace value, --ns value               cluster namespace (default: "liftbridge-default")
   --nats-servers ADDR[,ADDR], -n ADDR[,ADDR]  connect to NATS cluster at ADDR[,ADDR] (default: "nats://127.0.0.1:4222")
   --embedded-nats, -e                         run a NATS server embedded in this process
   --embedded-nats-config FILE, --nc FILE      load configuration for embedded NATS server from FILE
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

The configuration file uses a YAML format. The configuration settings are
described below. Settings follow a hierarchical pattern delimited by periods.
The full, flattened name or nested names can be used. For example:

```yaml
logging.level: debug
logging.recovery: true
logging.raft: true
```

The above configuration is equivalent to:

```yaml
logging:
  level: debug
  recovery: true
  raft: true
```

## Example Configuration File

An example configuration file is shown below.

```yaml
---
listen: localhost:9293
host: localhost
data.dir: /tmp/liftbridge/server-2
activity.stream.enabled: true

# Configure logging.
logging:
  level: debug
  raft: true

# Define NATS cluster to connect to.
nats.servers:
  - nats://localhost:4300
  - nats://localhost:4301

# Specify message stream settings.
streams:
  retention.max:
    age: 24h
    messages: 100
  compact.enabled: true

# Specify cluster settings.
clustering:
  server.id: server-2
  raft.bootstrap.seed: true
  replica.max.lag.time: 20s
```

## Overriding Configuration Settings with Environment Variables

For configuration set in the configuration file the value can be overridden
with environment variables prefixed with `LIFTBRIDGE_`. The key must exist in
the config file to be overridden.

For example using the config file from above one could override the host and
logging level with:

```sh
env LIFTBRIDGE_HOST=liftbridge.example.com \
  LIFTBRIDGE_LOGGING_LEVEL=error \
  liftbridge --config config.yaml
```

## Configuration Settings

Below is the list of Liftbridge configuration settings, including the name of
the setting in the configuration file and the CLI flag if it exists.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| listen | | The server listen host/port. This is the host and port the server will bind to. If this is not specified but `host` and `port` are specified, these values will be used. If neither `listen` nor `host`/`port` are specified, the default listen address will be used. | string | 0:0:0:0:9292  | |
| host | | The server host that is advertised to clients, i.e. the address clients will attempt to connect to based on metadata API responses. If not set, `listen` will be returned to clients. This value may differ from `listen` in situations where the external address differs from the internal address, e.g. when running in a container. If `listen` is not specified, the server will also bind to this host. | string | localhost | |
| port | port, p | The server port that is advertised to clients. See `host` for more information on how this behaves. | int | 9292 | |
| tls.key | tls-key | The private key file for server certificate. This must be set in combination with `tls.cert` to enable TLS. | string | |
| tls.cert | tls-cert | The server certificate file. This must be set in combination with `tls.key` to enable TLS. | string | |
| tls.client.auth.enabled |  | Enforce client-side authentication via certificate. | bool | false |
| tls.client.auth.ca |  | The CA certificate file to use when authenticating clients. | string | |
| tls.client.authz.enabled |  | Enable ACL authorization on streams. | bool | false |
| tls.client.authz.model | | (Deprecated) See [authorization docs.](./authentication_authorization.md#deprecated-use-a-file-to-store-acl-policies) ACL authorization configuration model. See [Casbin ACL example](https://github.com/casbin/casbin#examples) | string | |
| tls.client.authz.policy | | (Deprecated) See [authorization docs.](./authentication_authorization.md#deprecated-use-a-file-to-store-acl-policies) ACL authorization policy defenition file. See [Casbin ACL example](https://github.com/casbin/casbin#examples) | string | |
| logging.level | level, l | The logging level. | string | info | [debug, info, warn, error] |
| logging.recovery | | Log messages resulting from the replay of the Raft log on server recovery. | bool | false | |
| logging.raft | | Enables logging in the Raft subsystem. | bool | false | |
| logging.nats | | Enables logging for the embedded NATS server, if enabled (see [`nats.embedded`](#nats-configuration-settings)). | bool | false | |
| data.dir | data-dir, d | The directory to store data in. | string | /tmp/liftbridge/namespace | |
| batch.max.messages | | The maximum number of messages to batch when writing to disk. | int | 1024 |
| batch.max.time | | The maximum time to wait to batch more messages when writing to disk. | duration | 0 | |
| metadata.cache.max.age | | The maximum age of cached broker metadata. | duration | 2m | |
| nats | | NATS configuration. | map | | [See below](#nats-configuration-settings) |
| streams | | Write-ahead log configuration for message streams. | map | | [See below](#streams-configuration-settings) |
| clustering | | Broker cluster configuration. | map | | [See below](#clustering-configuration-settings) |
| activity | | Meta activity event stream configuration. | map | | [See below](#activity-configuration-settings) |
| cursors | | Cursor management configuration. | map | | [See below](#cursors-configuration-settings) |
| groups | | Consumer group configuration. | map | | [See below](#groups-configuration-settings) |

### NATS Configuration Settings

Below is the list of the configuration settings for the `nats` section of
the configuration file.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| servers | nats-servers, n | List of NATS hosts to connect to. | list | nats://localhost:4222 | |
| user | | Username to use to connect to NATS servers. | string | | |
| password | | Password to use to connect to NATS servers. | string | | |
| tls.cert | | Path to NATS certificate file. | string | | |
| tls.key | | Path to NATS key file. | string | | |
| tls.ca  | | Path to NATS CA Root file. | string | | |
| embedded | embedded-nats, e | Run a NATS server embedded in the process. | bool | false | |
| embedded.config | embedded-nats-config, nc | Path to [configuration file](https://docs.nats.io/nats-server/configuration) for embedded NATS server. | string | | |

### Streams Configuration Settings

Below is the list of the configuration settings for the `streams` section of the
configuration file. These settings are applied globally to all streams.
However, streams can be individually configured when they are created,
overriding these settings.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| retention.max.bytes | | The maximum size a stream's log can grow to, in bytes, before we will discard old log segments to free up space. A value of 0 indicates no limit. | int64 | 0 | |
| retention.max.messages | | The maximum size a stream's log can grow to, in number of messages, before we will discard old log segments to free up space. A value of 0 indicates no limit. | int64 | 0 | |
| retention.max.age | | The TTL for stream log segment files, after which they are deleted. A value of 0 indicates no TTL. | duration | 168h | |
| cleaner.interval | | The frequency to check if a new stream log segment file should be rolled and whether any segments are eligible for deletion based on the retention policy or compaction if enabled. | duration | 5m | |
| segment.max.bytes | | The maximum size of a single stream log segment file in bytes. Retention is always done a file at a time, so a larger segment size means fewer files but less granular control over retention. | int64 | 268435456 | |
| segment.max.age | | The maximum time before a new stream log segment is rolled out. A value of 0 means new segments will only be rolled when `segment.max.bytes` is reached. Retention is always done a file at a time, so a larger value means fewer files but less granular control over retention. | duration | value of `retention.max.age` | |
| compact.enabled | | Enables stream log compaction. Compaction works by retaining only the latest message for each key and discarding older messages. The frequency in which compaction runs is controlled by `cleaner.interval`. | bool | false | |
| compact.max.goroutines | | The maximum number of concurrent goroutines to use for compaction on a stream log (only applicable if `compact.enabled` is `true`). | int | 10 | |
| auto.pause.time | | The amount of time a stream partition can go idle, i.e. not receive a message, before it is automatically paused. A value of 0 disables auto pausing. | duration | 0 | |
| auto.pause.disable.if.subscribers | | Disables automatic stream partition pausing when there are subscribers. | bool | false | |
| concurrency.control | | Enable Optimistic Concurrency Control on message publishing for all streams. | bool | false | |
| encryption | | Enable encryption of data stored on server (encryption of data-at-rest). *NOTE: if enabled, an environment variable `LIFTBRIDGE_ENCRYPTION_KEY` must be set to a valid 128 bit or 256 bit AES key.* | bool | false | |

### Clustering Configuration Settings

Below is the list of the configuration settings for the `clustering` section of
the configuration file.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| server.id | server-id, id | ID of the server in the cluster. | string | random id | string with no spaces or periods |
| namespace | namespace, ns | Cluster namespace. | string | liftbridge-default | string with no spaces or periods |
| raft.snapshot.retain | | The number Raft log snapshots to retain on disk. | int | 2 | |
| raft.snapshot.threshold | | Controls how many outstanding logs there must be before taking a snapshot. This prevents excessive snapshots when a small set of logs can be replayed. | int | 8192 | |
| raft.cache.size | | The number of Raft logs to hold in memory for quick lookup. | int | 512 | |
| raft.bootstrap.seed | raft-bootstrap-seed | Bootstrap the Raft cluster by electing self as leader if there is no existing state. If this is enabled, `raft.bootstrap.peers` should generally not be used, either on this node or peer nodes, since cluster topology is not being explicitly defined. Instead, peers should be started without bootstrap flags which will cause them to automatically discover the bootstrapped leader and join the cluster. This is equivalent to setting `raft.bootstrap.peers` to be just this server, and it should only be enabled on one server in the cluster. | bool | false | |
| raft.bootstrap.peers | raft-bootstrap-peers | Bootstrap the Raft cluster with the provided list of peer IDs if there is no existing state. This should generally not be used in combination with `raft.bootstrap.seed` since it is explicitly defining cluster topology and the configured topology will elect a leader. Note that once the cluster is established, new nodes can join without setting bootstrap flags since they will automatically discover the elected leader and join the cluster. If `raft.bootstrap.peers` is set on multiple servers, it is recommended to set the full list of peers on each rather than a subset to avoid potential issues when setting `raft.max.quorum.size`. | list | | |
| raft.max.quorum.size | | The maximum number of servers to participate in the Raft quorum. Any servers added to the cluster beyond this number will participate as non-voters. Non-voter servers operate as normal but are not involved in the Raft election or commitment processes. Limiting this number allows the cluster to better scale since Raft requires a minimum of `N/2+1` nodes to perform operations. The should be set to the same value on all servers in the cluster. A value of 0 indicates no limit. | int | 0 | |
| replica.max.lag.time | | If a follower hasn't sent any replication requests or hasn't caught up to the leader's log end offset for at least this time, the leader will remove the follower from ISR. | duration | 15s | |
| replica.max.leader.timeout | | If a leader hasn't sent any replication responses for at least this time, the follower will report the leader to the controller. If a majority of the replicas report the leader, a new leader is selected by the controller. | duration | 15s | |
| replica.max.idle.wait | | The maximum amount of time a follower will wait before making a replication request once the follower is caught up with the leader. This value should always be less than `replica.max.lag.time` to avoid frequent shrinking of ISR for low-throughput streams. | duration | 10s | |
| replica.fetch.timeout | | Timeout duration for follower replication requests. | duration | 3s | |
| min.insync.replicas | | Specifies the minimum number of replicas that must acknowledge a stream write before it can be committed. If the ISR drops below this size, messages cannot be committed. | int | 1 | [1,...] |
| replication.max.bytes | | The maximum payload size, in bytes, a leader can send to followers for replication messages. This controls the amount of data that can be transferred for individual replication requests. If a leader receives a published message larger than this size, it will return an ack error to the client. Because replication is done over NATS, this cannot exceed the [`max_payload`](https://docs.nats.io/nats-server/configuration#limits) limit configured on the NATS cluster. Thus, this defaults to 1MB, which is the default value for `max_payload`. This should generally be set to match the value of `max_payload`. Setting it too low will preclude the replication of messages larger than it and negatively impact performance. This value should also be the same for all servers in the cluster. | int | 1048576 | |

### Activity Configuration Settings

Below is the list of the configuration settings for the `activity` section of
the configuration file.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| stream.enabled | | Enables the activity stream. This will create an internal stream called `__activity` which events will be published to. | bool | false | |
| stream.publish.timeout | | The timeout for publishes to the activity stream. This is the time to wait for an ack from the activity stream, which means it's related to `stream.publish.ack.policy`. If the ack policy is `none`, this has no effect.  | duration | 5s | |
| stream.publish.ack.policy | | The ack policy to use for publishes to the activity stream. The value `none` means publishes will not wait for an ack, `leader` means publishes will wait for the ack sent when the leader has committed the event, and `all` means publishes will wait for the ack sent when all replicas have committed the event. | string | all | [none, leader, all] |

### Cursors Configuration Settings

Below is the list of the configuration settings for the `cursors` section of
the configuration file.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| stream.partitions | | Sets the number of partitions for the internal `__cursors` stream which stores consumer cursors. A value of 0 disables the cursors stream. This cannot be changed once it is set. | int | 0 | |
| stream.replication.factor | | Sets the replication factor for the `__cursors` stream which stores consumer cursors. A value of -1 sets the replication factor to the number of servers in the cluster at the time the cursors stream is created. This cannot be changed once it is set. | int | -1 | |
| stream.auto.pause.time | | The amount of time a partition in the internal `__cursors` stream can go idle, i.e. not receive a cursor update or fetch, before it is automatically paused. A value of 0 disables auto pausing. | duration | 1m | |

### Consumer Groups Configuration Settings

Below is the list of the configuration settings for the `groups` section of
the configuration file.

| Name | Flag | Description | Type | Default | Valid Values |
|:----|:----|:----|:----|:----|:----|
| consumer.timeout | | If a consumer hasn't sent a request to fetch partition assignments to the group coordinator for at least this time, the coordinator will remove the consumer from the group. | duration | 15s | 
| coordinator.timeout | | If a group coordinator hasn't responded to assignment requests for at least this time, the member will report the coordinator to the controller. If a majority of the group members report the coordinator, a new coordinator is selected by the controller.| duration | 15s | |

