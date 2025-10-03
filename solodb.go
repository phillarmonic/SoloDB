package solodb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"time"
)

// Magic header bytes: "SOLODB\0\0"
var magicBytes = [8]byte{'S', 'O', 'L', 'O', 'D', 'B', 0, 0}

const (
	headerSize = 64
	version    = uint32(1)
)

// ValueType represents the type of value stored
type ValueType uint8

const (
	Int64T ValueType = iota
	Float64T
	BoolT
	StringT
	JSONT
	BlobT
	TombstoneT
)

// DurabilityMode controls sync behavior
type DurabilityMode int

const (
	SyncBatch DurabilityMode = iota
	SyncAlways
	SyncNever
)

// Errors
var (
	ErrNotFound     = errors.New("solodb: key not found")
	ErrExpired      = errors.New("solodb: key expired")
	ErrTypeMismatch = errors.New("solodb: type mismatch")
	ErrReadOnly     = errors.New("solodb: database is read-only")
	ErrCorrupt      = errors.New("solodb: data corruption detected")
	ErrClosed       = errors.New("solodb: database is closed")
)

// Options for opening a database
type Options struct {
	Path           string
	Durability     DurabilityMode
	PreallocateMiB int
	ReadOnly       bool
}

// Stats provides database statistics
type Stats struct {
	FileBytes         int64
	Keys              int
	LiveRecords       int
	LastCompactedUnix int64
}

// DB is the main database handle
type DB struct {
	mu     sync.RWMutex
	file   *os.File
	index  map[string]*recordMeta
	opts   Options
	closed bool

	// stats
	fileSize        int64
	lastCompactedAt int64

	// counters
	readsTotal    int64
	writesTotal   int64
	expiredOnRead int64
}

// recordMeta holds in-memory metadata about a record
type recordMeta struct {
	Offset       int64
	Length       uint32
	Type         ValueType
	InvalidAfter int64
}

// header represents the 64-byte file header
type header struct {
	Magic             [8]byte
	Version           uint32
	Flags             uint32
	CreatedAtUnix     int64
	LastCompactedUnix int64
	RecordCount       uint64
	// Remaining bytes are padding to reach 64 bytes
}

// record represents a single database record
type record struct {
	RecordLen    uint32
	CRC32C       uint32
	Type         ValueType
	InvalidAfter int64
	KeyLen       uint32
	ValLen       uint64
	Key          []byte
	Value        []byte
}

// Open opens or creates a SoloDB database
func Open(opts Options) (*DB, error) {
	if opts.Path == "" {
		return nil, errors.New("solodb: path cannot be empty")
	}

	// Open or create the file
	flags := os.O_RDWR | os.O_CREATE
	if opts.ReadOnly {
		flags = os.O_RDONLY
	}

	file, err := os.OpenFile(opts.Path, flags, 0644)
	if err != nil {
		return nil, fmt.Errorf("solodb: failed to open file: %w", err)
	}

	db := &DB{
		file:  file,
		index: make(map[string]*recordMeta),
		opts:  opts,
	}

	// Get file size
	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("solodb: failed to stat file: %w", err)
	}
	db.fileSize = fi.Size()

	// Initialize or validate header
	if db.fileSize == 0 {
		// New database - write header
		if !opts.ReadOnly {
			if err := db.writeHeader(); err != nil {
				_ = file.Close()
				return nil, err
			}
		}
	} else {
		// Existing database - validate header
		if err := db.validateHeader(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	// Scan file and build index
	if err := db.scanAndBuildIndex(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("solodb: scan failed: %w", err)
	}

	// Compact on open (GC expired and superseded records)
	if !opts.ReadOnly {
		if err := db.compactInternal(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("solodb: compaction failed: %w", err)
		}
	}

	return db, nil
}

// Close closes the database
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	db.closed = true
	return db.file.Close()
}

// writeHeader writes the initial header to a new database file
func (db *DB) writeHeader() error {
	h := header{
		Magic:             magicBytes,
		Version:           version,
		Flags:             0,
		CreatedAtUnix:     time.Now().Unix(),
		LastCompactedUnix: 0,
		RecordCount:       0,
	}

	buf := make([]byte, headerSize)
	copy(buf[0:8], h.Magic[:])
	binary.LittleEndian.PutUint32(buf[8:12], h.Version)
	binary.LittleEndian.PutUint32(buf[12:16], h.Flags)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(h.CreatedAtUnix))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.LastCompactedUnix))
	binary.LittleEndian.PutUint64(buf[32:40], h.RecordCount)

	if _, err := db.file.WriteAt(buf, 0); err != nil {
		return fmt.Errorf("solodb: failed to write header: %w", err)
	}

	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("solodb: failed to sync header: %w", err)
	}

	db.fileSize = headerSize
	return nil
}

// validateHeader validates the header of an existing database
func (db *DB) validateHeader() error {
	buf := make([]byte, headerSize)
	if _, err := db.file.ReadAt(buf, 0); err != nil {
		return fmt.Errorf("solodb: failed to read header: %w", err)
	}

	if !bytes.Equal(buf[0:8], magicBytes[:]) {
		return ErrCorrupt
	}

	ver := binary.LittleEndian.Uint32(buf[8:12])
	if ver != version {
		return fmt.Errorf("solodb: unsupported version %d", ver)
	}

	db.lastCompactedAt = int64(binary.LittleEndian.Uint64(buf[24:32]))
	return nil
}

// scanAndBuildIndex scans the file and builds the in-memory index
func (db *DB) scanAndBuildIndex() error {
	offset := int64(headerSize)
	now := time.Now().Unix()

	for offset < db.fileSize {
		rec, nextOffset, err := db.readRecordAt(offset)
		if err != nil {
			// Corruption detected - truncate file at this point if not read-only
			if !db.opts.ReadOnly {
				if err := db.file.Truncate(offset); err != nil {
					return fmt.Errorf("solodb: failed to truncate corrupted tail: %w", err)
				}
				db.fileSize = offset
			}
			break
		}

		// Skip expired records during scan
		if rec.InvalidAfter < now && rec.InvalidAfter != math.MaxInt64 {
			offset = nextOffset
			continue
		}

		// Update index with latest record per key
		key := string(rec.Key)
		db.index[key] = &recordMeta{
			Offset:       offset,
			Length:       rec.RecordLen + 4, // +4 for the record_len field itself
			Type:         rec.Type,
			InvalidAfter: rec.InvalidAfter,
		}

		offset = nextOffset
	}

	return nil
}

// readRecordAt reads a record at the given offset
func (db *DB) readRecordAt(offset int64) (*record, int64, error) {
	// Read record length (4 bytes)
	lenBuf := make([]byte, 4)
	if _, err := db.file.ReadAt(lenBuf, offset); err != nil {
		return nil, 0, err
	}
	recordLen := binary.LittleEndian.Uint32(lenBuf)

	// Sanity check
	if recordLen > 1<<30 { // 1GB max record size
		return nil, 0, ErrCorrupt
	}

	// Read full record
	fullLen := 4 + int64(recordLen)
	if offset+fullLen > db.fileSize {
		return nil, 0, ErrCorrupt
	}

	buf := make([]byte, fullLen)
	if _, err := db.file.ReadAt(buf, offset); err != nil {
		return nil, 0, err
	}

	// Parse record
	rec := &record{
		RecordLen: recordLen,
	}

	pos := 4 // skip record_len
	rec.CRC32C = binary.LittleEndian.Uint32(buf[pos : pos+4])
	pos += 4

	payloadStart := pos
	rec.Type = ValueType(buf[pos])
	pos++

	rec.InvalidAfter = int64(binary.LittleEndian.Uint64(buf[pos : pos+8]))
	pos += 8

	rec.KeyLen = binary.LittleEndian.Uint32(buf[pos : pos+4])
	pos += 4

	rec.ValLen = binary.LittleEndian.Uint64(buf[pos : pos+8])
	pos += 8

	rec.Key = buf[pos : pos+int(rec.KeyLen)]
	pos += int(rec.KeyLen)

	rec.Value = buf[pos : pos+int(rec.ValLen)]
	pos += int(rec.ValLen)

	// Verify CRC
	payload := buf[payloadStart:pos]
	computedCRC := crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
	if computedCRC != rec.CRC32C {
		return nil, 0, ErrCorrupt
	}

	return rec, offset + fullLen, nil
}

// Compact performs a full compaction of the database, removing expired and superseded records.
// This is called automatically on Open(), but can also be called manually to reclaim space.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}

	return db.compactInternal()
}

// compactInternal performs the actual compaction (must be called with lock held or during initialization)
func (db *DB) compactInternal() error {
	compactPath := db.opts.Path + ".compact"

	// Create temp compaction file
	compactFile, err := os.OpenFile(compactPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("solodb: failed to create compact file: %w", err)
	}
	defer func() { _ = compactFile.Close() }()

	// Write header
	h := header{
		Magic:             magicBytes,
		Version:           version,
		Flags:             0,
		CreatedAtUnix:     time.Now().Unix(),
		LastCompactedUnix: time.Now().Unix(),
		RecordCount:       uint64(len(db.index)),
	}

	buf := make([]byte, headerSize)
	copy(buf[0:8], h.Magic[:])
	binary.LittleEndian.PutUint32(buf[8:12], h.Version)
	binary.LittleEndian.PutUint32(buf[12:16], h.Flags)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(h.CreatedAtUnix))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.LastCompactedUnix))
	binary.LittleEndian.PutUint64(buf[32:40], h.RecordCount)

	if _, err := compactFile.Write(buf); err != nil {
		return err
	}

	// Write all live records from index
	newIndex := make(map[string]*recordMeta)
	offset := int64(headerSize)

	now := time.Now().Unix()
	for key, meta := range db.index {
		// Read original record
		rec, _, err := db.readRecordAt(meta.Offset)
		if err != nil {
			continue // skip corrupted records
		}

		// Skip tombstones during compaction
		if rec.Type == TombstoneT {
			continue
		}

		// Skip expired records during compaction
		if rec.InvalidAfter < now && rec.InvalidAfter != math.MaxInt64 {
			continue
		}

		// Write to compact file
		recBuf := db.encodeRecord(rec)
		if _, err := compactFile.Write(recBuf); err != nil {
			return err
		}

		// Update new index
		newIndex[key] = &recordMeta{
			Offset:       offset,
			Length:       uint32(len(recBuf)),
			Type:         rec.Type,
			InvalidAfter: rec.InvalidAfter,
		}

		offset += int64(len(recBuf))
	}

	// Sync compact file
	if err := compactFile.Sync(); err != nil {
		return err
	}

	// Close current file
	if err := db.file.Close(); err != nil {
		return err
	}

	// Atomic rename
	if err := os.Rename(compactPath, db.opts.Path); err != nil {
		return fmt.Errorf("solodb: failed to rename compact file: %w", err)
	}

	// Reopen file
	file, err := os.OpenFile(db.opts.Path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("solodb: failed to reopen file: %w", err)
	}

	db.file = file
	db.index = newIndex
	db.lastCompactedAt = h.LastCompactedUnix

	fi, _ := file.Stat()
	db.fileSize = fi.Size()

	return nil
}

// encodeRecord encodes a record into bytes
func (db *DB) encodeRecord(rec *record) []byte {
	// Calculate payload size
	payloadSize := 1 + 8 + 4 + 8 + int(rec.KeyLen) + int(rec.ValLen)

	buf := make([]byte, 4+4+payloadSize)

	// Write record_len
	binary.LittleEndian.PutUint32(buf[0:4], uint32(4+payloadSize))

	// Build payload for CRC
	payloadStart := 8
	buf[payloadStart] = byte(rec.Type)
	binary.LittleEndian.PutUint64(buf[payloadStart+1:payloadStart+9], uint64(rec.InvalidAfter))
	binary.LittleEndian.PutUint32(buf[payloadStart+9:payloadStart+13], rec.KeyLen)
	binary.LittleEndian.PutUint64(buf[payloadStart+13:payloadStart+21], rec.ValLen)
	copy(buf[payloadStart+21:], rec.Key)
	copy(buf[payloadStart+21+int(rec.KeyLen):], rec.Value)

	// Calculate and write CRC
	payload := buf[payloadStart : payloadStart+payloadSize]
	crc := crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[4:8], crc)

	return buf
}

// appendRecord appends a record to the file
func (db *DB) appendRecord(rec *record) error {
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}

	buf := db.encodeRecord(rec)

	offset := db.fileSize
	if _, err := db.file.WriteAt(buf, offset); err != nil {
		return fmt.Errorf("solodb: failed to write record: %w", err)
	}

	// Update index
	key := string(rec.Key)
	db.index[key] = &recordMeta{
		Offset:       offset,
		Length:       uint32(len(buf)),
		Type:         rec.Type,
		InvalidAfter: rec.InvalidAfter,
	}

	db.fileSize += int64(len(buf))
	db.writesTotal++

	// Sync based on durability mode
	if db.opts.Durability == SyncAlways {
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("solodb: failed to sync: %w", err)
		}
	}

	return nil
}

// SetInt stores an int64 value
func (db *DB) SetInt(key string, v int64, invalidAfter time.Time) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	valBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(valBuf, uint64(v))

	rec := &record{
		Type:         Int64T,
		InvalidAfter: invalidAfter.Unix(),
		KeyLen:       uint32(len(key)),
		ValLen:       8,
		Key:          []byte(key),
		Value:        valBuf,
	}

	return db.appendRecord(rec)
}

// SetFloat stores a float64 value
func (db *DB) SetFloat(key string, v float64, invalidAfter time.Time) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	valBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(valBuf, math.Float64bits(v))

	rec := &record{
		Type:         Float64T,
		InvalidAfter: invalidAfter.Unix(),
		KeyLen:       uint32(len(key)),
		ValLen:       8,
		Key:          []byte(key),
		Value:        valBuf,
	}

	return db.appendRecord(rec)
}

// SetBool stores a bool value
func (db *DB) SetBool(key string, v bool, invalidAfter time.Time) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var valByte byte
	if v {
		valByte = 1
	}

	rec := &record{
		Type:         BoolT,
		InvalidAfter: invalidAfter.Unix(),
		KeyLen:       uint32(len(key)),
		ValLen:       1,
		Key:          []byte(key),
		Value:        []byte{valByte},
	}

	return db.appendRecord(rec)
}

// SetString stores a string value
func (db *DB) SetString(key string, v string, invalidAfter time.Time) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rec := &record{
		Type:         StringT,
		InvalidAfter: invalidAfter.Unix(),
		KeyLen:       uint32(len(key)),
		ValLen:       uint64(len(v)),
		Key:          []byte(key),
		Value:        []byte(v),
	}

	return db.appendRecord(rec)
}

// SetJSON stores a JSON value
func (db *DB) SetJSON(key string, v json.RawMessage, invalidAfter time.Time) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rec := &record{
		Type:         JSONT,
		InvalidAfter: invalidAfter.Unix(),
		KeyLen:       uint32(len(key)),
		ValLen:       uint64(len(v)),
		Key:          []byte(key),
		Value:        v,
	}

	return db.appendRecord(rec)
}

// SetBlob stores a blob value
func (db *DB) SetBlob(key string, r io.Reader, size int64, invalidAfter time.Time) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Read blob into memory
	value := make([]byte, size)
	if _, err := io.ReadFull(r, value); err != nil {
		return fmt.Errorf("solodb: failed to read blob: %w", err)
	}

	rec := &record{
		Type:         BlobT,
		InvalidAfter: invalidAfter.Unix(),
		KeyLen:       uint32(len(key)),
		ValLen:       uint64(size),
		Key:          []byte(key),
		Value:        value,
	}

	return db.appendRecord(rec)
}

// Get retrieves a value by key (generic)
func (db *DB) Get(key string) (typ ValueType, data []byte, invalidAfter time.Time, err error) {
	db.mu.RLock()

	if db.closed {
		db.mu.RUnlock()
		return 0, nil, time.Time{}, ErrClosed
	}

	meta, ok := db.index[key]
	if !ok {
		db.mu.RUnlock()
		return 0, nil, time.Time{}, ErrNotFound
	}

	// Check expiration
	now := time.Now().Unix()
	if meta.InvalidAfter < now && meta.InvalidAfter != math.MaxInt64 {
		db.mu.RUnlock()

		// Lazy GC: upgrade to write lock and remove expired key from index
		db.mu.Lock()
		// Double-check it's still expired (race condition check)
		if meta, ok := db.index[key]; ok && meta.InvalidAfter < now && meta.InvalidAfter != math.MaxInt64 {
			delete(db.index, key)
		}
		db.mu.Unlock()

		// Track expired reads
		db.expiredOnRead++
		return 0, nil, time.Time{}, ErrExpired
	}

	// Read record
	rec, _, err := db.readRecordAt(meta.Offset)
	db.mu.RUnlock()

	if err != nil {
		return 0, nil, time.Time{}, err
	}

	db.readsTotal++
	return rec.Type, rec.Value, time.Unix(rec.InvalidAfter, 0), nil
}

// GetInt retrieves an int64 value
func (db *DB) GetInt(key string) (int64, error) {
	typ, data, _, err := db.Get(key)
	if err != nil {
		return 0, err
	}
	if typ != Int64T {
		return 0, ErrTypeMismatch
	}
	return int64(binary.LittleEndian.Uint64(data)), nil
}

// GetFloat retrieves a float64 value
func (db *DB) GetFloat(key string) (float64, error) {
	typ, data, _, err := db.Get(key)
	if err != nil {
		return 0, err
	}
	if typ != Float64T {
		return 0, ErrTypeMismatch
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(data)), nil
}

// GetBool retrieves a bool value
func (db *DB) GetBool(key string) (bool, error) {
	typ, data, _, err := db.Get(key)
	if err != nil {
		return false, err
	}
	if typ != BoolT {
		return false, ErrTypeMismatch
	}
	return data[0] == 1, nil
}

// GetString retrieves a string value
func (db *DB) GetString(key string) (string, error) {
	typ, data, _, err := db.Get(key)
	if err != nil {
		return "", err
	}
	if typ != StringT {
		return "", ErrTypeMismatch
	}
	return string(data), nil
}

// GetJSON retrieves a JSON value
func (db *DB) GetJSON(key string) (json.RawMessage, error) {
	typ, data, _, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	if typ != JSONT {
		return nil, ErrTypeMismatch
	}
	return json.RawMessage(data), nil
}

// GetBlob retrieves a blob value
func (db *DB) GetBlob(key string) (rc io.ReadCloser, size int64, invalidAfter time.Time, err error) {
	typ, data, invAfter, err := db.Get(key)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	if typ != BlobT {
		return nil, 0, time.Time{}, ErrTypeMismatch
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), invAfter, nil
}

// Delete marks a key as deleted (tombstone)
func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}

	rec := &record{
		Type:         TombstoneT,
		InvalidAfter: math.MaxInt64,
		KeyLen:       uint32(len(key)),
		ValLen:       0,
		Key:          []byte(key),
		Value:        nil,
	}

	if err := db.appendRecord(rec); err != nil {
		return err
	}

	// Remove from index
	delete(db.index, key)

	return nil
}

// Stats returns database statistics
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	liveRecords := 0
	for _, meta := range db.index {
		if meta.Type != TombstoneT {
			liveRecords++
		}
	}

	return Stats{
		FileBytes:         db.fileSize,
		Keys:              len(db.index),
		LiveRecords:       liveRecords,
		LastCompactedUnix: db.lastCompactedAt,
	}
}
