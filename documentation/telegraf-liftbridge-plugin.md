# Telegraf NATS Consumer Plugin - Liftbridge Extension

This document outlines a proposed contribution to extend Telegraf's existing NATS consumer input plugin with optional Liftbridge support.

## Background

Liftbridge uses NATS as its underlying transport layer. When messages are published to a Liftbridge stream, they flow through NATS subjects. The current Telegraf NATS consumer plugin can subscribe to these subjects and receive messages in real-time, but it cannot:

- Resume from a specific offset after restart
- Replay historical messages
- Use Liftbridge's consumer groups with durable cursors
- Track consumption progress

## Proposed Solution

Extend the existing `nats_consumer` input plugin with optional Liftbridge settings, following the same pattern used for JetStream support.

## Configuration

```toml
[[inputs.nats_consumer]]
  servers = ["nats://localhost:4222"]

  # Existing NATS options (unchanged)
  subjects = ["metrics.>"]
  queue_group = "telegraf"

  # NEW: Optional Liftbridge settings
  # When enabled, uses Liftbridge gRPC API instead of raw NATS for these streams
  liftbridge_enabled = false
  liftbridge_servers = ["localhost:9292"]

  # Streams to consume from Liftbridge (uses gRPC Subscribe API)
  liftbridge_streams = [
    { name = "metrics", partitions = [0, 1, 2] },
    { name = "events" },  # all partitions
  ]

  # Start position for new subscriptions
  # Options: "earliest", "latest", "new-only"
  liftbridge_start_position = "earliest"

  # Consumer group for durable offset tracking (optional)
  # When set, offsets are stored in Liftbridge and survive restarts
  liftbridge_consumer_group = "telegraf-metrics"

  # TLS settings for Liftbridge gRPC connection
  liftbridge_tls_cert = ""
  liftbridge_tls_key = ""
  liftbridge_tls_ca = ""
```

## Feature Comparison

| Feature | NATS Only | With Liftbridge |
|---------|-----------|-----------------|
| Real-time messages | Yes | Yes |
| Resume after restart | No | Yes |
| Historical replay | No | Yes |
| Durable offset tracking | No | Yes |
| Consumer groups | NATS queue groups | Liftbridge consumer groups |
| Partition awareness | No | Yes |

## Implementation Approach

### 1. Add Configuration Fields

```go
// In nats_consumer.go struct
type NatsConsumer struct {
    // ... existing fields ...

    // Liftbridge settings
    LiftbridgeEnabled       bool     `toml:"liftbridge_enabled"`
    LiftbridgeServers       []string `toml:"liftbridge_servers"`
    LiftbridgeStreams       []LiftbridgeStreamConfig `toml:"liftbridge_streams"`
    LiftbridgeStartPosition string   `toml:"liftbridge_start_position"`
    LiftbridgeConsumerGroup string   `toml:"liftbridge_consumer_group"`
    LiftbridgeTLSCert       string   `toml:"liftbridge_tls_cert"`
    LiftbridgeTLSKey        string   `toml:"liftbridge_tls_key"`
    LiftbridgeTLSCA         string   `toml:"liftbridge_tls_ca"`

    // Internal
    lbClient liftbridge.Client
    lbSubs   []context.CancelFunc
}

type LiftbridgeStreamConfig struct {
    Name       string `toml:"name"`
    Partitions []int  `toml:"partitions"` // empty = all partitions
}
```

### 2. Add Liftbridge Client Dependency

```go
import (
    liftbridge "github.com/liftbridge-io/go-liftbridge/v2"
)
```

### 3. Create Liftbridge Subscriptions

```go
func (n *NatsConsumer) createLiftbridgeSubscriptions() error {
    if !n.LiftbridgeEnabled {
        return nil
    }

    // Connect to Liftbridge
    client, err := liftbridge.Connect(n.LiftbridgeServers, n.liftbridgeClientOpts()...)
    if err != nil {
        return fmt.Errorf("failed to connect to Liftbridge: %w", err)
    }
    n.lbClient = client

    // Subscribe to each configured stream
    for _, streamCfg := range n.LiftbridgeStreams {
        partitions := streamCfg.Partitions
        if len(partitions) == 0 {
            // Get all partitions for this stream
            metadata, err := client.FetchMetadata(context.Background())
            if err != nil {
                return err
            }
            // ... extract partition IDs from metadata
        }

        for _, partition := range partitions {
            ctx, cancel := context.WithCancel(context.Background())
            n.lbSubs = append(n.lbSubs, cancel)

            opts := []liftbridge.SubscriptionOption{
                liftbridge.StartAtEarliestReceived(),
            }

            if n.LiftbridgeConsumerGroup != "" {
                // Use consumer group for durable cursors
                opts = append(opts,
                    liftbridge.ConsumerGroup(n.LiftbridgeConsumerGroup),
                    liftbridge.AutoAck(),
                )
            }

            go n.subscribeLiftbridge(ctx, streamCfg.Name, partition, opts)
        }
    }

    return nil
}

func (n *NatsConsumer) subscribeLiftbridge(ctx context.Context, stream string, partition int, opts []liftbridge.SubscriptionOption) {
    err := n.lbClient.Subscribe(ctx, stream, func(msg *liftbridge.Message, err error) {
        if err != nil {
            n.Log.Errorf("Liftbridge subscription error: %v", err)
            return
        }

        // Convert to the same format as NATS messages and send to existing channel
        n.in <- &nats.Msg{
            Subject: msg.Subject(),
            Data:    msg.Value(),
            Header:  n.liftbridgeHeadersToNATS(msg.Headers()),
        }
    }, opts...)

    if err != nil && err != context.Canceled {
        n.Log.Errorf("Liftbridge subscribe failed: %v", err)
    }
}
```

### 4. Handle Cleanup

```go
func (n *NatsConsumer) Stop() {
    // Cancel Liftbridge subscriptions
    for _, cancel := range n.lbSubs {
        cancel()
    }

    if n.lbClient != nil {
        n.lbClient.Close()
    }

    // ... existing NATS cleanup
}
```

## Benefits of This Approach

1. **Code reuse**: Leverages existing NATS consumer infrastructure (message channel, parser, tracking)
2. **Familiar configuration**: Users already using NATS consumer just add a few lines
3. **Backward compatible**: Existing configs continue to work unchanged
4. **Optional dependency**: Liftbridge client only imported when feature is enabled
5. **Consistent patterns**: Follows the JetStream extension model already in the plugin

## Files to Modify

| File | Changes |
|------|---------|
| `plugins/inputs/nats_consumer/nats_consumer.go` | Add Liftbridge config fields and subscription logic |
| `plugins/inputs/nats_consumer/sample.conf` | Add Liftbridge configuration examples |
| `plugins/inputs/nats_consumer/README.md` | Document Liftbridge options |
| `go.mod` | Add `github.com/liftbridge-io/go-liftbridge/v2` dependency |

## Testing Strategy

1. **Unit tests**: Mock Liftbridge client, verify subscription creation
2. **Integration tests**: Run against local Liftbridge cluster
3. **Compatibility tests**: Ensure existing NATS-only configs still work

## Future Enhancements

- Support for Liftbridge's cursor API for manual offset management
- Partition rebalancing callbacks
- Metrics for Liftbridge-specific stats (lag, partition distribution)

## References

- [Liftbridge Go Client](https://github.com/liftbridge-io/go-liftbridge)
- [Telegraf NATS Consumer Plugin](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/nats_consumer)
- [Telegraf Contributing Guide](https://github.com/influxdata/telegraf/blob/master/CONTRIBUTING.md)
