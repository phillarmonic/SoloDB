# 🎵 SoloDB

**SoloDB** is a single-file, embedded key-value store for Go with optional expiration support. Simple, fast, and crash-safe.

## Features

- ✨ **Single file** - One append-only file with automatic compaction
- 🎯 **Type-safe** - Native support for `int64`, `float64`, `bool`, `string`, `json.RawMessage`, and binary blobs
- ⏰ **Optional expiration** - Every record can have an expiration time (or use `math.MaxInt64` for no expiration)
- 🔄 **Automatic GC** - Lazy GC on reads + manual `Compact()` - no restarts needed
- 💾 **Crash-safe** - Append-only writes with CRC32C checksums and atomic compaction
- 🚀 **Fast** - In-memory index for O(1) lookups, sequential writes for performance
- 🔒 **Concurrent** - Single writer, multiple readers with RWMutex
- 🎛️ **Configurable durability** - Choose between SyncAlways, SyncBatch, or SyncNever

## Installation

```bash
go get github.com/phillarmonic/SoloDB
```

## Quick Start

```go
package main

import (
    "math"
    "time"
    "github.com/phillarmonic/SoloDB"
)

func main() {
    // Open database
    db, err := solodb.Open(solodb.Options{
        Path:       "app.solo",
        Durability: solodb.SyncBatch,
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // No expiration (recommended default)
    noExpire := time.Unix(math.MaxInt64, 0)

    // Store values
    db.SetString("greeting", "Hello, World!", noExpire)
    db.SetInt("counter", 42, noExpire)
    db.SetBool("enabled", true, noExpire)

    // Retrieve values
    greeting, _ := db.GetString("greeting")
    counter, _ := db.GetInt("counter")
    enabled, _ := db.GetBool("enabled")

    // With expiration (1 hour)
    expiry := time.Now().Add(1 * time.Hour)
    db.SetString("session:123", "active", expiry)

    // Delete a key
    db.Delete("counter")

    // Get stats
    stats := db.Stats()
}
```

## API Reference

### Opening a Database

```go
type Options struct {
    Path           string         // File path for the database
    Durability     DurabilityMode // SyncAlways, SyncBatch, or SyncNever
    PreallocateMiB int           // Optional preallocation (future use)
    ReadOnly       bool          // Open in read-only mode
}

db, err := solodb.Open(Options{Path: "data.solo"})
```

### Setting Values

All `Set*` methods require an expiration time. Use `time.Unix(math.MaxInt64, 0)` for no expiration (recommended default).

```go
noExpire := time.Unix(math.MaxInt64, 0)

db.SetInt(key string, v int64, invalidAfter time.Time) error
db.SetFloat(key string, v float64, invalidAfter time.Time) error
db.SetBool(key string, v bool, invalidAfter time.Time) error
db.SetString(key string, v string, invalidAfter time.Time) error
db.SetJSON(key string, v json.RawMessage, invalidAfter time.Time) error
db.SetBlob(key string, r io.Reader, size int64, invalidAfter time.Time) error
```

### Getting Values

```go
db.GetInt(key string) (int64, error)
db.GetFloat(key string) (float64, error)
db.GetBool(key string) (bool, error)
db.GetString(key string) (string, error)
db.GetJSON(key string) (json.RawMessage, error)
db.GetBlob(key string) (io.ReadCloser, int64, time.Time, error)

// Generic get (returns type, bytes, expiration)
db.Get(key string) (ValueType, []byte, time.Time, error)
```

### Other Operations

```go
// Delete a key (writes tombstone)
db.Delete(key string) error

// Manually trigger compaction (GC)
// Note: GC also runs automatically on Open() and lazily on reads of expired keys
db.Compact() error

// Get database statistics
stats := db.Stats()
// Stats{ FileBytes, Keys, LiveRecords, LastCompactedUnix }

// Close the database
db.Close() error
```

### Errors

```go
solodb.ErrNotFound      // Key does not exist
solodb.ErrExpired       // Key has expired
solodb.ErrTypeMismatch  // Wrong type for Get* operation
solodb.ErrReadOnly      // Cannot write in read-only mode
solodb.ErrCorrupt       // Data corruption detected
solodb.ErrClosed        // Database is closed
```

## How It Works

### File Format

SoloDB uses a simple, efficient file format:

```
[Header: 64 bytes] [Record 1] [Record 2] ...
```

**Header (64 bytes):**
- Magic: `SOLODB\0\0` (8 bytes)
- Version: u32
- Flags: u32 (reserved)
- CreatedAtUnix: i64
- LastCompactedUnix: i64
- RecordCount: u64
- Padding to 64 bytes

**Record format (append-only):**
- `record_len`: u32 (bytes from here to end)
- `crc32c`: u32 (CRC32C of payload)
- `type`: u8 (0=int64, 1=float64, 2=bool, 3=string, 4=json, 5=blob, 6=tombstone)
- `invalid_after_unix`: i64 (expiration timestamp)
- `key_len`: u32
- `val_len`: u64
- `key`: bytes
- `value`: bytes

All integers are little-endian. Checksums use CRC32C.

### Lifecycle

1. **Open**: Scans file, validates records, builds in-memory index
2. **Compact**: Removes expired and superseded records, writes to `.compact` temp file
3. **Atomic Rename**: Swaps compacted file with original
4. **Serve**: Ready for reads/writes with in-memory index
5. **Lazy GC**: Expired keys are automatically removed from index on read
6. **Manual Compact**: Call `db.Compact()` anytime to reclaim disk space

### Durability Modes

- **SyncAlways**: `fsync` after every write (safest, slowest)
- **SyncBatch**: Batch syncs (default, balanced)
- **SyncNever**: No automatic sync (fastest, caller manages)

### Crash Safety

- Append-only writes with CRC32C validation
- Startup scanner truncates corrupted tail records
- Compaction writes to temporary file with atomic rename
- Original file remains valid until rename completes

## Examples

See the [example](./example/) directory for complete working examples:

### Basic Usage
```bash
cd example/basic
go run main.go
```

### Expiration Demo
Watch keys expire in real-time with automatic GC:
```bash
cd example/expiration
go run main.go
```

This demo:
- Sets a key with 5-second expiration
- Queries it every 500ms until it expires
- Shows lazy GC removing expired keys from index automatically
- Demonstrates manual compaction to reclaim disk space
- No database restarts needed!

## Testing

```bash
# Run all tests
go test -v

# Run benchmarks
go test -bench=.

# Run with race detector
go test -race
```

## Performance Considerations

- **Sequential writes** - Fast on SSDs due to append-only design
- **O(1) lookups** - In-memory index for fast reads
- **Compaction** - O(n) with single pass, triggered on startup
- **No background threads** - Simple, predictable behavior

## Limitations

- Single writer (multiple readers OK)
- No range scans (yet - see roadmap)
- No compression (yet - see roadmap)
- Large blobs stored inline (may split in future)

## Roadmap (Post v0.1)

- [ ] AES-GCM encryption at rest
- [ ] zstd compression per record
- [ ] Range/prefix scans
- [ ] `SetWithTTL` convenience method
- [ ] Optional mmap for larger datasets
- [ ] Background compaction option

## License

See [LICENSE](./LICENSE) file.

## Why "SoloDB"?

A play on "solo" (single-file) with a musical touch 🎵. Simple, standalone, and gets the job done.

---

Built with ❤️ for Go developers who need a simple, embedded key-value store.
