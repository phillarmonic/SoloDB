SoloDB — Spec (v0.1)
1) Purpose

A single-file, embedded key–value store for Go that supports:

primitives: int64, float64, bool, string

structured: json.RawMessage

binary/file blobs: arbitrary bytes

every record carries an optional expiration field (invalid_after_unix), an absolute Unix epoch when the value becomes invalid. Use math.MaxInt64 for no expiration (recommended for most use cases).

DB is a short-lived process: on startup, it runs GC (drops expired & superseded records and compacts) before serving any query.

2) High-level design

Single append-only file with periodic full compaction.

On open:

Scan file → rebuild in-memory index (latest live record per key), ignore expired.

Compact immediately to a fresh file (or in-place rewrite strategy) so only live, latest records remain.

Swap to compacted file and start serving.

In-memory index: map[string]recordMeta (offset, length, type, invalid_after_unix).

Single writer, multi-reader with RWMutex. No background threads required.

3) File format

All integers are little-endian. Checksums use CRC32C.

| Header | Records... |

3.1 Header (fixed 64 bytes)

Magic: SOLODB\0 (8 bytes)

Version: u32 (currently 1)

Flags: u32 (bitfield; reserved)

CreatedAtUnix: i64

LastCompactedUnix: i64

RecordCount: u64 (best-effort, updated on compaction)

Reserved/Padding: fill to 64 bytes

3.2 Record layout (append-only)
u32  record_len                    // bytes from here to end of record (exclude this field)
u32  crc32c_of_payload             // crc of [type|invalid_after|key_len|val_len|key|value|json_hint|blob_meta]
u8   type                          // 0=int64, 1=float64, 2=bool, 3=string, 4=json, 5=blob, 6=tombstone
i64  invalid_after_unix            // absolute unix epoch when record becomes invalid
u32  key_len
u64  val_len                       // len of value payload (for blob: bytes length)
[ u8 key[key_len] ]
[ u8 value[val_len] ]              // encoding depends on type (see below)
[ optional extras ]


Type encodings

int64: value is 8 bytes.

float64: value is 8 bytes (IEEE 754).

bool: value is 1 byte (0/1).

string: UTF-8 bytes.

json: raw JSON bytes (no framing).

blob: raw bytes (the actual file/content).

tombstone: val_len=0.

Note: We always write invalid_after_unix. "No expiration" is represented by the sentinel value math.MaxInt64 (recommended default).

4) Expiration model

Every record has invalid_after_unix populated at write time.

Read path must verify time.Now().Unix() < invalid_after_unix.

On startup GC, expired records are dropped.

Lazy GC on read: when a key is found expired during read, it's automatically removed from the in-memory index (does not require restart or manual compaction).

5) Compaction (GC)

Trigger: 
- Automatic: always on Open() before serving queries
- Manual: call db.Compact() anytime to reclaim disk space
- Lazy: expired keys removed from index on read

Strategy:

Stream original file, keep only latest non-expired record per key.

Write to a new temp file (.compact), then fsync, then atomic rename to the primary path.

Guarantees:

Crash safety: original file remains valid until rename completes.

After success: header fields updated (RecordCount, LastCompactedUnix).

6) Indexing

Build an in-memory map[string]recordMeta at open, after compaction:

type recordMeta struct {
    Offset int64
    Length uint32
    Type   ValueType
    InvalidAfter int64
}


Lookups read directly via pread (no mmap required but allowed).

No secondary indexes (keep it simple).

7) Concurrency & durability

Concurrency: RWMutex

Get: RLock

Set/Delete: Lock

Durability modes:

SyncAlways: fdatasync after each write (slower, safest)

SyncBatch: sync on a timer or N records (default)

SyncNever: caller manages process lifecycle (fastest)

Writes are append-only; a crash may leave a partial tail record. On open, the scanner:

validates record_len bounds and crc32c

stops at first corruption; truncates file to last valid offset.

8) Public Go API (sketch)
package solodb

import (
    "context"
    "time"
    "encoding/json"
)

type DB struct { /* internal fields */ }

type Options struct {
    Path           string
    Durability     DurabilityMode
    PreallocateMiB int    // optional
    ReadOnly       bool
}

type DurabilityMode int
const (
    SyncBatch DurabilityMode = iota
    SyncAlways
    SyncNever
)

type ValueType uint8
const (
    Int64T ValueType = iota
    Float64T
    BoolT
    StringT
    JSONT
    BlobT
)

type Stats struct {
    FileBytes         int64
    Keys              int
    LiveRecords       int
    LastCompactedUnix int64
}

func Open(opts Options) (*DB, error)           // scans, GC/compact, builds index
func (db *DB) Close() error

// Set* APIs: you provide an absolute expiration time.
// Use time.Unix(math.MaxInt64, 0) for "no expire" (recommended default).
func (db *DB) SetInt(key string, v int64, invalidAfter time.Time) error
func (db *DB) SetFloat(key string, v float64, invalidAfter time.Time) error
func (db *DB) SetBool(key string, v bool, invalidAfter time.Time) error
func (db *DB) SetString(key string, v string, invalidAfter time.Time) error
func (db *DB) SetJSON(key string, v json.RawMessage, invalidAfter time.Time) error

// Blob I/O
func (db *DB) SetBlob(key string, r io.Reader, size int64, invalidAfter time.Time) error
func (db *DB) GetBlob(key string) (rc io.ReadCloser, size int64, invalidAfter time.Time, err error)

func (db *DB) Get(key string) (typ ValueType, bytes []byte, invalidAfter time.Time, err error)
// Convenience typed getters (validate not expired)
func (db *DB) GetInt(key string) (int64, error)
func (db *DB) GetFloat(key string) (float64, error)
func (db *DB) GetBool(key string) (bool, error)
func (db *DB) GetString(key string) (string, error)
func (db *DB) GetJSON(key string) (json.RawMessage, error)

func (db *DB) Delete(key string) error  // writes tombstone with MaxInt64 invalid_after (or immediate expiry)

func (db *DB) Compact() error  // manually trigger full compaction to reclaim disk space

func (db *DB) Stats() Stats


Notes

Callers pass absolute invalidAfter times (e.g., time.Now().Add(24*time.Hour)).

All reads verify not expired; if expired, return ErrExpired (and optionally schedule lazy cleanup).

9) Errors

ErrNotFound

ErrExpired

ErrTypeMismatch

ErrReadOnly

ErrCorrupt

ErrClosed

10) Performance considerations

Sequential append for writes (fast on SSDs).

Optional preallocation (fallocate) to reduce fragmentation.

Optional mmap for faster reads; default uses pread for portability.

Compaction complexity O(n) with a single pass + final write.

11) Crash safety

Append record → write → fsync (per mode).

Compaction writes to path.tmp → fsync → atomic rename to path.

Startup scanner truncates trailing garbage safely.

12) Config & tuning

Durability (Always/Batch/Never)

PreallocateMiB

ReadOnly

Future: MaxValueInlineBytes (if you later split blob pages vs inline)

13) Observability

Lightweight counters (atomic):

reads_total, writes_total, expired_on_read, gc_dropped, crc_failures

Optional hooks:

type Hooks struct {
    OnOpenScanStart func()
    OnOpenScanEnd   func(stats ScanStats)
    OnCompactStart  func()
    OnCompactEnd    func(bytesBefore, bytesAfter int64)
}


Stats() snapshot.

14) Testing matrix

Power-loss simulation: kill during write / during compaction.

Large blobs (≥100MB).

Mixed types, heavy churn, high key collision.

Time-rollover for expiry.

Corrupted tail records.

Concurrent R/W under race detector.

15) Roadmap (post-v0.1)

Optional AES-GCM at rest (key supplied by host app).

Compression (zstd) per record (flag bit + dict id).

Range scan (prefix iteration) by maintaining an auxiliary sorted key list on compact.

TTL convenience: SetWithTTL(key, v, ttl) → computes invalid_after_unix.

16) Minimal lifecycle example
db, err := solodb.Open(solodb.Options{Path: "app.solo", Durability: solodb.SyncBatch})
if err != nil { panic(err) }
defer db.Close()

// GC already ran before this line.

exp := time.Now().Add(6 * time.Hour)
_ = db.SetString("session:123", "alice", exp)

name, err := db.GetString("session:123") // ErrExpired if past exp
// ...

_ = db.Delete("session:123")

s := db.Stats()

17) Naming/branding notes

Keep the product “musical but functional”:

SoloDB as the package/module name.

Minor musical easter eggs in docs (e.g., “compaction cadence”), but API stays clean and literal.