package solodb

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"path/filepath"
	"testing"
	"time"
)

func TestBasicSetGet(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{
		Path:       dbPath,
		Durability: SyncAlways,
	})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// No expiration
	noExpire := time.Unix(math.MaxInt64, 0)

	// Test Int64
	if err := db.SetInt("counter", 42, noExpire); err != nil {
		t.Fatalf("SetInt failed: %v", err)
	}
	val, err := db.GetInt("counter")
	if err != nil {
		t.Fatalf("GetInt failed: %v", err)
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}

	// Test Float64
	if err := db.SetFloat("pi", 3.14159, noExpire); err != nil {
		t.Fatalf("SetFloat failed: %v", err)
	}
	fval, err := db.GetFloat("pi")
	if err != nil {
		t.Fatalf("GetFloat failed: %v", err)
	}
	if fval != 3.14159 {
		t.Errorf("expected 3.14159, got %f", fval)
	}

	// Test Bool
	if err := db.SetBool("enabled", true, noExpire); err != nil {
		t.Fatalf("SetBool failed: %v", err)
	}
	bval, err := db.GetBool("enabled")
	if err != nil {
		t.Fatalf("GetBool failed: %v", err)
	}
	if !bval {
		t.Errorf("expected true, got false")
	}

	// Test String
	if err := db.SetString("name", "Alice", noExpire); err != nil {
		t.Fatalf("SetString failed: %v", err)
	}
	sval, err := db.GetString("name")
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if sval != "Alice" {
		t.Errorf("expected Alice, got %s", sval)
	}

	// Test JSON
	jsonData := json.RawMessage(`{"user":"bob","age":30}`)
	if err := db.SetJSON("user:1", jsonData, noExpire); err != nil {
		t.Fatalf("SetJSON failed: %v", err)
	}
	jval, err := db.GetJSON("user:1")
	if err != nil {
		t.Fatalf("GetJSON failed: %v", err)
	}
	if !bytes.Equal(jval, jsonData) {
		t.Errorf("JSON mismatch: expected %s, got %s", jsonData, jval)
	}
}

func TestBlob(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	noExpire := time.Unix(math.MaxInt64, 0)

	// Write blob
	data := []byte("This is a test blob with some binary data: \x00\x01\x02\xff")
	reader := bytes.NewReader(data)
	if err := db.SetBlob("file:test", reader, int64(len(data)), noExpire); err != nil {
		t.Fatalf("SetBlob failed: %v", err)
	}

	// Read blob
	rc, size, _, err := db.GetBlob("file:test")
	if err != nil {
		t.Fatalf("GetBlob failed: %v", err)
	}
	defer func() { _ = rc.Close() }()

	if size != int64(len(data)) {
		t.Errorf("size mismatch: expected %d, got %d", len(data), size)
	}

	readData, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("failed to read blob: %v", err)
	}

	if !bytes.Equal(readData, data) {
		t.Errorf("blob data mismatch")
	}
}

func TestExpiration(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Set with immediate expiration
	expired := time.Now().Add(-1 * time.Second)
	if err := db.SetString("expired", "should-be-gone", expired); err != nil {
		t.Fatalf("SetString failed: %v", err)
	}

	// Should get ErrExpired
	_, err = db.GetString("expired")
	if err != ErrExpired {
		t.Errorf("expected ErrExpired, got %v", err)
	}

	// Set with future expiration
	future := time.Now().Add(1 * time.Hour)
	if err := db.SetString("valid", "still-here", future); err != nil {
		t.Fatalf("SetString failed: %v", err)
	}

	val, err := db.GetString("valid")
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if val != "still-here" {
		t.Errorf("expected still-here, got %s", val)
	}
}

func TestDelete(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	noExpire := time.Unix(math.MaxInt64, 0)

	// Set a value
	if err := db.SetString("key", "value", noExpire); err != nil {
		t.Fatalf("SetString failed: %v", err)
	}

	// Verify it exists
	if _, err := db.GetString("key"); err != nil {
		t.Fatalf("GetString failed: %v", err)
	}

	// Delete it
	if err := db.Delete("key"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Should get ErrNotFound
	_, err = db.GetString("key")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestTypeMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	noExpire := time.Unix(math.MaxInt64, 0)

	// Set as int
	if err := db.SetInt("num", 42, noExpire); err != nil {
		t.Fatalf("SetInt failed: %v", err)
	}

	// Try to get as string
	_, err = db.GetString("num")
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestCompaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	// Create DB and write multiple versions of same key
	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	noExpire := time.Unix(math.MaxInt64, 0)

	// Write same key multiple times
	for i := 0; i < 10; i++ {
		if err := db.SetInt("counter", int64(i), noExpire); err != nil {
			t.Fatalf("SetInt failed: %v", err)
		}
	}

	// Write an expired key
	expired := time.Now().Add(-1 * time.Hour)
	if err := db.SetString("old", "expired", expired); err != nil {
		t.Fatalf("SetString failed: %v", err)
	}

	stats1 := db.Stats()
	_ = db.Close()

	// Reopen - should trigger compaction
	db, err = Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to reopen db: %v", err)
	}
	defer func() { _ = db.Close() }()

	stats2 := db.Stats()

	// File should be smaller after compaction
	if stats2.FileBytes >= stats1.FileBytes {
		t.Errorf("expected file to shrink after compaction: before=%d, after=%d",
			stats1.FileBytes, stats2.FileBytes)
	}

	// Should have correct value
	val, err := db.GetInt("counter")
	if err != nil {
		t.Fatalf("GetInt failed: %v", err)
	}
	if val != 9 {
		t.Errorf("expected 9, got %d", val)
	}

	// Expired key should not exist
	_, err = db.GetString("old")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for expired key, got %v", err)
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	noExpire := time.Unix(math.MaxInt64, 0)

	// Write data
	{
		db, err := Open(Options{Path: dbPath})
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}

		if err := db.SetString("persistent", "data", noExpire); err != nil {
			t.Fatalf("SetString failed: %v", err)
		}

		if err := db.SetInt("count", 100, noExpire); err != nil {
			t.Fatalf("SetInt failed: %v", err)
		}

		_ = db.Close()
	}

	// Reopen and verify
	{
		db, err := Open(Options{Path: dbPath})
		if err != nil {
			t.Fatalf("failed to reopen db: %v", err)
		}
		defer func() { _ = db.Close() }()

		val, err := db.GetString("persistent")
		if err != nil {
			t.Fatalf("GetString failed: %v", err)
		}
		if val != "data" {
			t.Errorf("expected 'data', got %s", val)
		}

		count, err := db.GetInt("count")
		if err != nil {
			t.Fatalf("GetInt failed: %v", err)
		}
		if count != 100 {
			t.Errorf("expected 100, got %d", count)
		}
	}
}

func TestReadOnly(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	noExpire := time.Unix(math.MaxInt64, 0)

	// Create and populate DB
	{
		db, err := Open(Options{Path: dbPath})
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}

		if err := db.SetString("key", "value", noExpire); err != nil {
			t.Fatalf("SetString failed: %v", err)
		}

		_ = db.Close()
	}

	// Open read-only
	{
		db, err := Open(Options{Path: dbPath, ReadOnly: true})
		if err != nil {
			t.Fatalf("failed to open db read-only: %v", err)
		}
		defer func() { _ = db.Close() }()

		// Should be able to read
		val, err := db.GetString("key")
		if err != nil {
			t.Fatalf("GetString failed: %v", err)
		}
		if val != "value" {
			t.Errorf("expected 'value', got %s", val)
		}

		// Should not be able to write
		err = db.SetString("new", "value", noExpire)
		if err != ErrReadOnly {
			t.Errorf("expected ErrReadOnly, got %v", err)
		}

		// Should not be able to delete
		err = db.Delete("key")
		if err != ErrReadOnly {
			t.Errorf("expected ErrReadOnly, got %v", err)
		}
	}
}

func TestStats(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	noExpire := time.Unix(math.MaxInt64, 0)

	// Write some data
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		if err := db.SetInt(key, int64(i), noExpire); err != nil {
			t.Fatalf("SetInt failed: %v", err)
		}
	}

	stats := db.Stats()

	if stats.Keys != 5 {
		t.Errorf("expected 5 keys, got %d", stats.Keys)
	}

	if stats.LiveRecords != 5 {
		t.Errorf("expected 5 live records, got %d", stats.LiveRecords)
	}

	if stats.FileBytes <= headerSize {
		t.Errorf("expected file bytes > header size, got %d", stats.FileBytes)
	}
}

func TestLargeValue(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	noExpire := time.Unix(math.MaxInt64, 0)

	// Create a 1MB string
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	if err := db.SetBlob("large", bytes.NewReader(largeValue), int64(len(largeValue)), noExpire); err != nil {
		t.Fatalf("SetBlob failed: %v", err)
	}

	rc, size, _, err := db.GetBlob("large")
	if err != nil {
		t.Fatalf("GetBlob failed: %v", err)
	}
	defer func() { _ = rc.Close() }()

	if size != int64(len(largeValue)) {
		t.Errorf("size mismatch: expected %d, got %d", len(largeValue), size)
	}

	readData, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("failed to read blob: %v", err)
	}

	if !bytes.Equal(readData, largeValue) {
		t.Errorf("large value mismatch")
	}
}

func TestMultipleKeys(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	noExpire := time.Unix(math.MaxInt64, 0)

	// Write 100 keys
	for i := 0; i < 100; i++ {
		key := string(rune('k')) + string(rune(i))
		if err := db.SetInt(key, int64(i*10), noExpire); err != nil {
			t.Fatalf("SetInt failed for key %s: %v", key, err)
		}
	}

	// Read them back
	for i := 0; i < 100; i++ {
		key := string(rune('k')) + string(rune(i))
		val, err := db.GetInt(key)
		if err != nil {
			t.Fatalf("GetInt failed for key %s: %v", key, err)
		}
		if val != int64(i*10) {
			t.Errorf("key %s: expected %d, got %d", key, i*10, val)
		}
	}
}

func TestNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.GetString("nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestEmptyKey(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	noExpire := time.Unix(math.MaxInt64, 0)

	// Empty key should work
	if err := db.SetString("", "empty-key-value", noExpire); err != nil {
		t.Fatalf("SetString with empty key failed: %v", err)
	}

	val, err := db.GetString("")
	if err != nil {
		t.Fatalf("GetString with empty key failed: %v", err)
	}
	if val != "empty-key-value" {
		t.Errorf("expected 'empty-key-value', got %s", val)
	}
}

func TestManualCompact(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	noExpire := time.Unix(9223372036854775807, 0)

	// Write multiple versions of same key
	for i := 0; i < 5; i++ {
		if err := db.SetInt("counter", int64(i), noExpire); err != nil {
			t.Fatalf("SetInt failed: %v", err)
		}
	}

	// Write an expired key
	expired := time.Now().Add(-1 * time.Hour)
	if err := db.SetString("old", "expired", expired); err != nil {
		t.Fatalf("SetString failed: %v", err)
	}

	statsBeforeCompact := db.Stats()

	// Manually trigger compaction
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	statsAfterCompact := db.Stats()

	// File should be smaller after compaction
	if statsAfterCompact.FileBytes >= statsBeforeCompact.FileBytes {
		t.Errorf("expected file to shrink after compaction: before=%d, after=%d",
			statsBeforeCompact.FileBytes, statsAfterCompact.FileBytes)
	}

	// Should have correct value
	val, err := db.GetInt("counter")
	if err != nil {
		t.Fatalf("GetInt failed: %v", err)
	}
	if val != 4 {
		t.Errorf("expected 4, got %d", val)
	}

	// Expired key should not exist after compaction
	_, err = db.GetString("old")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for expired key after compaction, got %v", err)
	}
}

func TestLazyGC(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.solo")

	db, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Set a key with immediate expiration
	expired := time.Now().Add(-1 * time.Second)
	if err := db.SetString("expired", "should-be-gone", expired); err != nil {
		t.Fatalf("SetString failed: %v", err)
	}

	// Set a valid key
	noExpire := time.Unix(9223372036854775807, 0)
	if err := db.SetString("valid", "still-here", noExpire); err != nil {
		t.Fatalf("SetString failed: %v", err)
	}

	// Index should have 2 keys
	stats := db.Stats()
	if stats.Keys != 2 {
		t.Errorf("expected 2 keys, got %d", stats.Keys)
	}

	// Try to read expired key - should trigger lazy GC
	_, err = db.GetString("expired")
	if err != ErrExpired {
		t.Errorf("expected ErrExpired, got %v", err)
	}

	// After lazy GC, index should have 1 key
	stats = db.Stats()
	if stats.Keys != 1 {
		t.Errorf("expected 1 key after lazy GC, got %d", stats.Keys)
	}

	// Valid key should still exist
	val, err := db.GetString("valid")
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if val != "still-here" {
		t.Errorf("expected 'still-here', got %s", val)
	}
}

func BenchmarkSetInt(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.solo")

	db, err := Open(Options{
		Path:       dbPath,
		Durability: SyncNever,
	})
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	noExpire := time.Unix(math.MaxInt64, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := string(rune('k')) + string(rune(i%1000))
		_ = db.SetInt(key, int64(i), noExpire)
	}
}

func BenchmarkGetInt(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.solo")

	db, err := Open(Options{
		Path:       dbPath,
		Durability: SyncNever,
	})
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	noExpire := time.Unix(math.MaxInt64, 0)

	// Populate with 1000 keys
	for i := 0; i < 1000; i++ {
		key := string(rune('k')) + string(rune(i))
		_ = db.SetInt(key, int64(i), noExpire)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := string(rune('k')) + string(rune(i%1000))
		_, _ = db.GetInt(key)
	}
}
