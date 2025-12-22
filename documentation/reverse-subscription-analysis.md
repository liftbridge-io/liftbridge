# Feasibility Analysis: Reverse Subscription (Issue #298)

## Summary

**Feature**: Add `reverse` boolean to `SubscribeRequest` to read messages in reverse chronological order (newest to oldest).

**Verdict**: ✅ **HIGHLY FEASIBLE** - The index structure already supports reverse lookups. Implementation requires ~200-300 lines of new code with no breaking changes.

---

## Use Cases

### 1. Consumer Cursor Fetching (Primary Motivation)

**Current Problem** (`server/cursors.go:217-266`):
```go
// TODO: This can likely be made more efficient.
sub, err := c.api.SubscribeInternal(ctx, &client.SubscribeRequest{
    Stream:        cursorsStream,
    Partition:     partition.Id,
    StartPosition: client.StartPosition_EARLIEST,  // ← Scans from BEGINNING
    Resume:        true,
})
```

- Scans **ALL messages** from beginning to find latest cursor value
- O(n) complexity where n = total messages in cursor partition
- For large deployments: millions of messages scanned for one lookup

**With Reverse Reading**:
```go
sub, err := c.api.SubscribeInternal(ctx, &client.SubscribeRequest{
    Stream:        cursorsStream,
    Partition:     partition.Id,
    StartPosition: client.StartPosition_LATEST,
    Reverse:       true,  // ← NEW: Read backwards
})
// First match = latest cursor value → immediate return
```

- O(k) complexity where k = updates per cursor (typically 1-10)
- **1000x-1,000,000x faster** for cursor lookups

### 2. User-Facing Reverse Subscriptions

Users want to subscribe to streams in reverse order for:
- Displaying latest events first in UIs
- Finding recent occurrences of specific events
- Debugging by examining recent messages

---

## Technical Feasibility

### Index Structure: ✅ Supports Reverse

**File**: `server/commitlog/index.go`

```
Entry Structure (20 bytes fixed):
├── Offset     (4 bytes) - Relative offset from baseOffset
├── Timestamp  (8 bytes) - Unix nanoseconds
├── Position   (4 bytes) - Byte position in log file
└── Size       (4 bytes) - Message size in bytes
```

**Why it works**:
- Fixed-size entries → direct address calculation: `fileOffset = entryIndex * 20`
- Memory-mapped → O(1) random access
- Entries sorted by offset → binary search works both directions
- Can iterate backwards: `entry[n-1]`, `entry[n-2]`, ...

### Segment ReadAt: ✅ Already Position-Agnostic

**File**: `server/commitlog/segment.go:365-375`

```go
func (s *segment) ReadAt(p []byte, off int64) (n int, err error) {
    return s.log.ReadAt(p, off)  // Works with ANY byte offset
}
```

No sequential constraint - can read from end or middle.

### Current Forward-Only Assumptions

| Location | Assumption | Change Needed |
|----------|------------|---------------|
| `reader.go:90` | `r.offset = offset + 1` | Add reverse mode: `r.offset = offset - 1` |
| `reader.go:130` | `findSegmentByBaseOffset(baseOffset+1)` | Add `findPreviousSegment()` |
| `index.go:323` | `s.offset++` in scanner | Add reverse scanner with `s.offset--` |

---

## Pros and Cons

### Pros

| Benefit | Impact |
|---------|--------|
| **Massive cursor lookup speedup** | 1000x+ faster for `getLatestCursorOffset()` |
| **User-requested feature** | Issue #298 with maintainer approval |
| **No breaking changes** | `reverse=false` (default) = existing behavior |
| **Minimal code changes** | ~200-300 lines, mostly additive |
| **Index already supports it** | No format changes needed |
| **Backward compatible** | Existing clients/protos unchanged |

### Cons / Risks

| Risk | Mitigation |
|------|------------|
| **Complexity in reader code** | Keep separate `ReverseReader` to avoid branching in hot path |
| **High watermark semantics** | For committed reads, start from HW and go backwards |
| **Stop condition logic** | Need clear semantics for when reverse iteration ends |
| **Testing surface** | Need comprehensive tests for edge cases |
| **Proto change in external repo** | Requires PR to `liftbridge-api` repo first |

---

## Effort Estimate

### Phase 1: Core Implementation (Medium Effort)

| Task | Files | Lines | Effort |
|------|-------|-------|--------|
| Add `Reverse` to SubscribeRequest proto | `liftbridge-api` repo | ~5 | Low |
| Implement reverse index scanner | `index.go` | ~30 | Low |
| Implement reverse segment scanner | `segment.go` | ~40 | Medium |
| Add reverse reader mode | `reader.go` | ~80 | Medium |
| Handle reverse in partition.Subscribe() | `partition.go` | ~50 | Medium |
| Handle reverse in api.Subscribe() | `api.go` | ~20 | Low |

**Subtotal**: ~225 lines, **Medium effort**

### Phase 2: Cursor Optimization (Low Effort)

| Task | Files | Lines | Effort |
|------|-------|-------|--------|
| Update `getLatestCursorOffset()` | `cursors.go` | ~15 | Low |
| Add early-exit on first match | `cursors.go` | ~10 | Low |

**Subtotal**: ~25 lines, **Low effort**

### Phase 3: Testing & Documentation (Medium Effort)

| Task | Files | Lines | Effort |
|------|-------|-------|--------|
| Unit tests for reverse scanner | `index_test.go` | ~50 | Medium |
| Unit tests for reverse reader | `reader_test.go` | ~80 | Medium |
| Integration tests | `partition_test.go` | ~100 | Medium |
| Documentation | `README.md`, docs | ~50 | Low |

**Subtotal**: ~280 lines, **Medium effort**

### Total Estimate

- **Code**: ~530 lines (225 core + 25 cursor + 280 tests)
- **Effort**: Medium
- **Risk**: Low (additive changes, no format changes)
- **Dependencies**: PR to `liftbridge-api` repo for proto change

---

## Implementation Plan

### Step 1: Proto Change (External)

Add to `liftbridge-api/go/api.proto`:
```protobuf
message SubscribeRequest {
    // ... existing fields ...
    bool reverse = 11;  // Read messages in reverse order
}
```

### Step 2: Reverse Index Scanner

**File**: `server/commitlog/index.go`

```go
type reverseIndexScanner struct {
    idx    *index
    entry  *entry
    offset int64  // Current entry index (starts at end)
}

func (s *reverseIndexScanner) Scan() (*entry, error) {
    if s.offset < 0 {
        return nil, io.EOF
    }
    err := s.idx.ReadEntryAtLogOffset(s.entry, s.offset)
    if err != nil {
        return nil, err
    }
    s.offset--  // Decrement for reverse
    return s.entry, nil
}
```

### Step 3: Reverse Reader

**File**: `server/commitlog/reader.go`

```go
func (l *commitLog) NewReader(offset int64, uncommitted bool, reverse bool) (*Reader, error) {
    if reverse {
        return l.newReverseReader(offset, uncommitted)
    }
    return l.newForwardReader(offset, uncommitted)
}

// Reverse reader decrements offset instead of incrementing
func (r *reverseReader) ReadMessage(ctx context.Context, ...) (...) {
    // Read message at current offset
    // Decrement: r.offset--
    // Navigate to previous segment when needed
}
```

### Step 4: Partition Subscribe Handler

**File**: `server/partition.go:367-454`

```go
func (p *partition) Subscribe(ctx context.Context, req *client.SubscribeRequest) (*subscription, error) {
    var reader commitlog.Reader
    if req.Reverse {
        startOffset := p.getStartOffsetForReverse(req)
        reader, err = p.log.NewReader(startOffset, false, true)  // reverse=true
    } else {
        startOffset := p.getStartOffset(req)
        reader, err = p.log.NewReader(startOffset, false, false)
    }
    // ... rest unchanged
}
```

### Step 5: Cursor Optimization

**File**: `server/cursors.go:217-266`

```go
func (c *cursorManager) getLatestCursorOffset(...) (int64, error) {
    sub, err := c.api.SubscribeInternal(ctx, &client.SubscribeRequest{
        Stream:        cursorsStream,
        Partition:     partition.Id,
        StartPosition: client.StartPosition_LATEST,
        Reverse:       true,  // NEW
    })

    for {
        select {
        case msg := <-msgC:
            if bytes.Equal(msg.Key, cursorKey) {
                // First match when reading backwards = latest!
                cursor.Unmarshal(msg.Value)
                return cursor.Offset, nil  // EARLY EXIT
            }
        // ...
        }
    }
}
```

---

## Semantic Clarifications

| Scenario | Behavior |
|----------|----------|
| `reverse=true`, `StartPosition=LATEST` | Start at newest message, read backwards |
| `reverse=true`, `StartPosition=EARLIEST` | Start at oldest, read backwards (unusual but valid) |
| `reverse=true`, `StartPosition=OFFSET(X)` | Start at offset X, read backwards |
| `reverse=true`, `StopPosition=OFFSET(Y)` | Stop when reaching offset Y (going backwards) |
| `reverse=true`, committed reader | Start at HW, read backwards to oldest |
| `reverse=false` (default) | Current behavior unchanged |

---

## Files to Modify

| File | Changes |
|------|---------|
| `liftbridge-api` (external) | Add `reverse` field to `SubscribeRequest` |
| `server/commitlog/index.go` | Add `reverseIndexScanner` |
| `server/commitlog/segment.go` | Add `reverseSegmentScanner` |
| `server/commitlog/reader.go` | Add reverse reader mode |
| `server/commitlog/commitlog.go` | Update `NewReader()` signature |
| `server/partition.go` | Handle `Reverse` flag in `Subscribe()` |
| `server/api.go` | Pass `Reverse` through to partition |
| `server/cursors.go` | Optimize `getLatestCursorOffset()` |

---

## Recommendation

**Proceed with implementation.** This is a well-scoped feature with:
- Clear use case (cursor optimization + user request)
- Low risk (additive changes only)
- Strong technical foundation (index already supports it)
- Maintainer approval (Tyler Treat approved in issue #298)

The cursor optimization alone justifies the feature, and exposing it to users is a natural extension.
