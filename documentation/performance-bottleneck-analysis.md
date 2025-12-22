# Liftbridge Performance Bottleneck Analysis

## Executive Summary

This analysis identifies critical performance bottlenecks in Liftbridge's publish and retrieval code paths. The findings are categorized by severity and include specific code locations.

---

## PUBLISH PATH BOTTLENECKS

### Critical: Lock Contention

| Bottleneck | Location | Impact |
|------------|----------|--------|
| **Partition RWMutex** | `partition.go:123` | Serializes ALL replica offset reads; held during ISR iteration |
| **Segment Write Mutex** | `segment.go:334` | Single write mutex serializes ALL writes for a partition |
| **Index RWMutex** | `index.go:128-145` | Lock held during mmap write operations |
| **CommitLog RWMutex** | `commitlog.go:225-226` | Concurrent publish and split operations compete |
| **PublishAsync Inflight Mutex** | `api.go:1060,1163` | Serializes ack tracking for every publish |

### Critical: Memory Allocations in Hot Path

| Allocation | Location | Frequency |
|------------|----------|-----------|
| `new(bytes.Buffer)` for batch | `message_set.go:60` | Every batch append |
| `new(bytes.Buffer)` for index | `index.go:129` | Every batch append |
| `make(map[string][]byte)` headers | `partition.go:1831` | **Every message** |
| `make([]int64, isrSize)` | `partition.go:1355` | Every commit check |
| `&client.Message{}` | `api.go:405-415` | Every publish |

### High: Serialization Overhead (Double Encoding)

```
API Publish (api.go:862)
    → proto.MarshalPublish(msg)           // First marshal
    → NATS transport
    → proto.UnmarshalPublish(data)        // Unmarshal (partition.go:1817)
    → Header map iteration/copy           // partition.go:1837-1839
    → binary.Write() x5 per message       // message_set.go:82-101
```

**Impact**: Messages are serialized multiple times with unnecessary intermediate allocations.

### High: Synchronous I/O

| Operation | Location | Impact |
|-----------|----------|--------|
| Sync publish blocks for ack | `api.go:881-909` | Round-trip latency per message |
| Direct file write (no buffering) | `segment.go:348` | OS may block if cache full |
| Index mmap write | `index.go:141` | Kernel manages page flushing |

### Medium: Segment Split Contention

```go
// commitlog.go:669-690 - CAS retry loop with NO BACKOFF
for {
    activeSegment := l.activeSegment()
    if !activeSegment.CheckSplit(l.MaxSegmentAge) {
        return false, nil
    }
    if err := l.split(activeSegment); err != nil {
        if err == ErrSegmentExists {
            continue  // SPIN-WAIT - no backoff!
        }
    }
}
```

### Medium: ISR Replication Synchronization

```go
// partition.go:1330-1391 - commitLoop
// Must wait for ALL ISR replicas to catch up (slowest replica)
// No partial commits for faster replicas
pending.(*client.Ack).Offset <= minLatest  // Line 1366
```

---

## RETRIEVAL PATH BOTTLENECKS

### Critical: One-at-a-Time Message Processing

```go
// partition.go:458-549 - newSubscribeLoop
for {
    m, offset, timestamp, _, err := reader.ReadMessage(ctx, headersBuf)
    // ... serialize ...
    select {
    case ch <- msg:  // Send ONE message at a time
    case <-cancel:
        return
    }
}
```

**Impact**: No batching on read side; each `ReadMessage()` performs individual segment I/O with lock acquisition.

### Critical: Per-Message Memory Allocations

| Allocation | Location | Frequency |
|------------|----------|-----------|
| `make([]byte, size)` | `message_set.go:126` | **Every message read** |
| `&client.Message{}` | `partition.go:522` | **Every message** |
| `m.Headers()` map | `message.go:127` | **Every message** |
| `string()` conversions | `partition.go:530-531` | **Every message** |

### High: Reader Lock Contention

```go
// reader.go:101-169 - uncommittedReader.Read
func (r *uncommittedReader) Read(ctx context.Context, p []byte) (n int, err error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    // ENTIRE LOOP holds lock - even waiting operations
}
```

**Impact**: Readers waiting for data block lock acquisition for other readers.

### High: No gRPC Batching

```go
// api.go:224-261 - Subscribe
for {
    select {
    case m := <-msgC:
        if err := out.Send(m); err != nil {  // ONE message per Send()
            return err
        }
    }
}
```

**Impact**: Full TCP/gRPC framing overhead per message; network underutilization.

### High: Repeated Segment List Fetches

```go
// reader.go - Called repeatedly in loop
segments = r.cl.Segments()  // Allocates new slice copy each time
// ... later in same function ...
segments = r.cl.Segments()  // Called again!
```

**Impact**: O(n) allocation on every segment boundary crossing.

### Medium: Index Binary Search Per Message

```go
// segment.go:502-507
// Every message requires binary search in index
// Each search requires individual ReadEntryAtFileOffset() with lock
```

---

## COMMITLOG INTERNALS BOTTLENECKS

### Architectural Decision: No fsync on Log Writes (Under Review)

```go
// segment.go:344-362
func (s *segment) write(p []byte, entries []*entry) (n int, err error) {
    n, err = s.writer.Write(p)  // Buffered write - NO fsync
    s.position += int64(n)
    s.notifyWaiters()  // Readers see data immediately ✓
    return n, nil
}
```

**Current behavior**: Data goes to OS kernel buffer, readers see it immediately (correct for streaming).

**Trade-off**: Durability relies on ISR replication rather than fsync. Acceptable for most streaming use cases.

**Status**: Under review - see "Configurable Sync Policy" proposal below for optional stronger durability.

### Critical: HW Not Flushed on Update

```go
// commitlog.go:395 - TODO comment in code!
// TODO: should we flush the HW to disk here?
func (l *commitLog) SetHighWatermark(hw int64) {
    l.mu.Lock()
    if hw > l.hw {
        l.hw = hw
        l.notifyHWChange()  // Notify but DON'T fsync
    }
    l.mu.Unlock()
}
```

**Risk**: HW only checkpointed every 5 seconds (default). Up to 5 seconds of HW progress lost on crash.

### High: Index File Expansion Blocks Writes

```go
// index.go:189-211
if offset+pSize >= idx.size {
    // DURING WRITES under lock:
    idx.file.Truncate(newSize)      // Blocks I/O
    oldMmap.UnsafeUnmap()           // Expensive
    idx.mmap, _ = gommap.Map(...)   // New mapping
}
```

### High: Double-Buffering in Message Encoding

```
Message → bytes.Buffer (message_set.go:60)
       → binary.Write() to buffer
       → buffer.Bytes()
       → segment.writer.Write() (another buffer)
       → OS kernel buffer
       → Disk
```

**Impact**: 3-4 memory copies before data reaches disk.

### Medium: HW Waiter Notification is O(n)

```go
// commitlog.go:408-415
func (l *commitLog) notifyHWChange() {
    for r, ch := range l.hwWaiters {
        ch <- false           // Individual channel send
        delete(l.hwWaiters, r)
    }
}
```

---

## SEVERITY RANKING

### P0 - Critical (Data Loss Risk)
1. **No fsync on log writes** - Crash loses kernel buffer data
2. **HW checkpoint only every 5s** - 5-second durability window

### P1 - Critical (Performance)
1. **Per-message memory allocations** - GC pressure under load
2. **Segment write mutex** - Serializes all partition writes
3. **One-at-a-time message reads** - No read batching
4. **Double encoding on publish** - Unnecessary serialization

### P2 - High
1. **No gRPC batching** - Network underutilization
2. **Index expansion blocks writes** - mmap re-creation overhead
3. **Reader mutex held for entire loop** - Lock contention
4. **Repeated Segments() calls** - Allocation per boundary

### P3 - Medium
1. **CAS retry with no backoff** - Spin-wait on segment split
2. **ISR waits for slowest replica** - No partial commits
3. **HW notification O(n)** - Scales poorly with readers
4. **Index binary search per read** - Lock per lookup

---

## RECOMMENDED OPTIMIZATIONS

### Quick Wins (Low Effort, High Impact)
1. **sync.Pool for bytes.Buffer** - Reuse buffers in hot paths
2. **Batch gRPC sends** - Accumulate N messages before Send()
3. **Cache Segments() result** - Don't re-fetch on every read
4. **Add backoff to CAS retry** - Prevent spin-wait

### Medium Effort
1. **Eliminate double encoding** - Pass raw bytes through NATS
2. **Read batching** - Fetch N messages per ReadMessage call
3. **Lock-free HW reads** - Use atomic operations
4. **Pre-allocate header maps** - Use fixed-size arrays

### High Effort (Architectural)
1. **Write-ahead buffer** - Batch writes before segment append
2. **Async index writes** - Decouple from message path
3. **Configurable fsync policy** - Every N messages or timer
4. **Lock-free segment access** - Atomic pointers for segment list

---

## KEY FILES TO MODIFY

| File | Bottlenecks |
|------|-------------|
| `server/api.go` | gRPC batching, publish mutex |
| `server/partition.go` | ISR locks, message processing loop |
| `server/commitlog/commitlog.go` | HW flushing, segment locks |
| `server/commitlog/segment.go` | Write mutex, fsync policy |
| `server/commitlog/index.go` | Buffer allocation, expansion |
| `server/commitlog/message_set.go` | Double-buffering |
| `server/commitlog/reader.go` | Read batching, lock scope |

---

## PROPOSED FEATURE: Configurable Sync Policy

### Rationale

The current design has two characteristics:
1. **No fsync on message writes** - Messages go to OS kernel buffer, not disk
2. **HW checkpoint every 5 seconds** - Background goroutine writes HW to file

This is acceptable for streaming (readers see data immediately), but users may want stronger durability guarantees without full WAL complexity.

### Design: Configurable Fsync Policy (Kafka-inspired)

Instead of implementing a full WAL (which adds write amplification and complexity), provide a configurable sync policy that lets users choose their durability/performance trade-off.

#### New Configuration

```yaml
# liftbridge.yaml
streams:
  sync:
    messages: 0        # fsync every N messages (0 = disabled)
    interval: 0s       # fsync every interval (0 = disabled)
    on.hw.update: false # fsync when HW advances
```

#### Sync Policy Presets

| Preset | sync.messages | sync.interval | Use Case |
|--------|---------------|---------------|----------|
| **throughput** (default) | 0 | 0 | Maximum performance, rely on ISR replication |
| **balanced** | 0 | 1s | Fsync every second, good balance |
| **durable** | 100 | 500ms | Fsync every 100 msgs OR 500ms |
| **strict** | 1 | 0 | Fsync every message (slowest) |

#### Implementation

**File: `server/commitlog/options.go`**
```go
type SyncPolicy struct {
    Messages   int           // fsync every N messages (0 = never)
    Interval   time.Duration // fsync every interval (0 = never)
    OnHWUpdate bool          // fsync when HW advances
}
```

**File: `server/commitlog/segment.go`**
```go
type segment struct {
    // ... existing fields ...
    syncPolicy     SyncPolicy
    messagesSinceSync int
    lastSyncTime   time.Time
}

func (s *segment) write(p []byte, entries []*entry) (n int, err error) {
    n, err = s.writer.Write(p)
    if err != nil {
        return n, err
    }
    s.position += int64(n)

    // Check sync policy
    s.messagesSinceSync += len(entries)
    if s.shouldSync() {
        if err := s.sync(); err != nil {
            return n, err
        }
    }

    s.notifyWaiters()
    return n, nil
}

func (s *segment) shouldSync() bool {
    if s.syncPolicy.Messages > 0 && s.messagesSinceSync >= s.syncPolicy.Messages {
        return true
    }
    if s.syncPolicy.Interval > 0 && time.Since(s.lastSyncTime) >= s.syncPolicy.Interval {
        return true
    }
    return false
}

func (s *segment) sync() error {
    if err := s.log.Sync(); err != nil {
        return err
    }
    s.messagesSinceSync = 0
    s.lastSyncTime = time.Now()
    return nil
}
```

**File: `server/commitlog/commitlog.go`**
```go
func (l *commitLog) SetHighWatermark(hw int64) {
    l.mu.Lock()
    if hw > l.hw {
        l.hw = hw
        l.notifyHWChange()

        // Optional: sync on HW update
        if l.syncPolicy.OnHWUpdate {
            l.checkpointHW()
        }
    }
    l.mu.Unlock()
}
```

#### Benefits Over Full WAL

| Aspect | Full WAL | Configurable Sync |
|--------|----------|-------------------|
| Write amplification | 2x (WAL + segment) | 1x |
| Complexity | High (rotation, compaction, recovery) | Low |
| Recovery | Replay WAL | Scan segment for last valid offset |
| Flexibility | All or nothing | User chooses trade-off |
| Backward compatible | No | Yes (default = current behavior) |

#### Migration Path

1. **Phase 1**: Add `SyncPolicy` config, default to current behavior (no sync)
2. **Phase 2**: Add metrics for sync latency and frequency
3. **Phase 3**: Document trade-offs in user guide
4. **Future**: If users need stronger guarantees, consider WAL as opt-in feature

### Files to Modify

| File | Changes |
|------|---------|
| `server/commitlog/options.go` | Add `SyncPolicy` struct |
| `server/commitlog/segment.go` | Add sync tracking and `shouldSync()` logic |
| `server/commitlog/commitlog.go` | Pass sync policy to segments, optional HW sync |
| `server/config.go` | Add `streams.sync.*` configuration |
| `documentation/configuration.md` | Document new options |

---

## PROPOSED FEATURE: Hot Configuration Reload

### Rationale

Currently, any configuration change requires a full server restart. For production deployments (especially Kubernetes), hot reload via SIGHUP would allow tuning without downtime.

**Good news**: SIGHUP handling already exists in `server/signal.go` - it currently only reloads Casbin authorization. We can extend this pattern.

### Configuration Categories

| Category | Hot-Reload Safety | Notes |
|----------|-------------------|-------|
| **Logging levels** | ✅ SAFE | Read on each log call |
| **Batch sizes** | ✅ SAFE | Read during publish |
| **Activity stream** | ✅ SAFE | Runtime direct access |
| **Replication timeouts** | ⚠️ PARTIAL | Some cached at replicator creation |
| **Retention policies** | ⚠️ PARTIAL | Baked into partition commitlogs |
| **Authorization (Casbin)** | ✅ ALREADY DONE | Reload on SIGHUP exists |
| **Network/Ports** | ❌ UNSAFE | Requires restart |
| **Clustering IDs** | ❌ UNSAFE | Persisted to disk |
| **Data directory** | ❌ UNSAFE | Commitlogs created at startup |
| **TLS certs** | ❌ UNSAFE | Listener initialized once |

### Implementation Phases

#### Phase 1: Safe Values (Low Risk, Quick Win)

Extend SIGHUP handler in `server/signal.go` to reload:
- `logging.level`, `logging.recovery`, `logging.raft`, `logging.nats`
- `batch.max.messages`, `batch.max.time`
- `activity.stream.*` settings
- `clustering.replica.max.leader.timeout` (read per-access)
- `clustering.replica.max.idle.wait` (read per-access)
- `clustering.replica.fetch.timeout` (read per-access)

```go
case syscall.SIGHUP:
    // Existing Casbin reload...

    // NEW: Reload safe config values
    newConfig, err := server.NewConfig(configFilePath)
    if err != nil {
        s.logger.Errorf("Failed to parse config: %v", err)
        continue
    }

    s.mu.Lock()
    s.config.LogLevel = newConfig.LogLevel
    s.config.BatchMaxMessages = newConfig.BatchMaxMessages
    s.config.BatchMaxTime = newConfig.BatchMaxTime
    s.config.Clustering.ReplicaMaxLeaderTimeout = newConfig.Clustering.ReplicaMaxLeaderTimeout
    // ... other safe values
    s.mu.Unlock()

    s.logger.Info("Configuration reloaded")
```

#### Phase 2: Coordinated Reload (Medium Effort)

For values cached in running components:
- **Retention policies**: Signal partition cleaners to re-read config
- **MinISR changes**: Trigger ISR recalculation for all partitions
- **Replicator timeouts**: Consider recreating active replicators

#### Phase 3: Documentation

Document which settings require restart:
- Network (port, listen, NATS servers)
- Clustering (server.id, namespace, raft bootstrap)
- Storage (data.dir)
- TLS (all cert/key paths)

### Files to Modify

| File | Changes |
|------|---------|
| `server/signal.go` | Extend SIGHUP handler for config reload |
| `server/server.go` | Add `ReloadConfig()` method with locking |
| `server/config.go` | Add `Reload()` method to parse and validate |
| `documentation/configuration.md` | Document hot-reloadable vs restart-required settings |

### Safety Requirements

1. **Config read lock**: Use `Server.mu.RLock()` when accessing mutable config
2. **Atomic updates**: Update related values together
3. **Validation**: Validate new config before applying
4. **Fallback**: Log errors but don't crash if reload fails
