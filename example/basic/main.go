package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	solodb "github.com/phillarmonic/SoloDB"
)

func main() {
	// Open database with SyncBatch durability
	db, err := solodb.Open(solodb.Options{
		Path:       "app.solo",
		Durability: solodb.SyncBatch,
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	fmt.Println("✨ SoloDB - Single-file embedded key-value store")
	fmt.Println()

	// No expiration (recommended default)
	noExpire := time.Unix(math.MaxInt64, 0)

	// Store different types
	fmt.Println("📝 Storing values...")

	// Int64
	if err := db.SetInt("user:count", 42, noExpire); err != nil {
		log.Fatalf("SetInt failed: %v", err)
	}
	fmt.Println("  ✓ Stored int64: user:count = 42")

	// Float64
	if err := db.SetFloat("pi", 3.14159, noExpire); err != nil {
		log.Fatalf("SetFloat failed: %v", err)
	}
	fmt.Println("  ✓ Stored float64: pi = 3.14159")

	// Bool
	if err := db.SetBool("feature:enabled", true, noExpire); err != nil {
		log.Fatalf("SetBool failed: %v", err)
	}
	fmt.Println("  ✓ Stored bool: feature:enabled = true")

	// String
	if err := db.SetString("greeting", "Hello, SoloDB!", noExpire); err != nil {
		log.Fatalf("SetString failed: %v", err)
	}
	fmt.Println("  ✓ Stored string: greeting = 'Hello, SoloDB!'")

	// JSON
	user := map[string]interface{}{
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   30,
	}
	userJSON, _ := json.Marshal(user)
	if err := db.SetJSON("user:alice", userJSON, noExpire); err != nil {
		log.Fatalf("SetJSON failed: %v", err)
	}
	fmt.Println("  ✓ Stored JSON: user:alice")

	// With expiration (expires in 1 hour)
	sessionExpiry := time.Now().Add(1 * time.Hour)
	if err := db.SetString("session:abc123", "active", sessionExpiry); err != nil {
		log.Fatalf("SetString failed: %v", err)
	}
	fmt.Printf("  ✓ Stored session with 1-hour expiry\n")

	fmt.Println()
	fmt.Println("📖 Reading values...")

	// Read back values
	count, _ := db.GetInt("user:count")
	fmt.Printf("  user:count = %d\n", count)

	pi, _ := db.GetFloat("pi")
	fmt.Printf("  pi = %f\n", pi)

	enabled, _ := db.GetBool("feature:enabled")
	fmt.Printf("  feature:enabled = %v\n", enabled)

	greeting, _ := db.GetString("greeting")
	fmt.Printf("  greeting = '%s'\n", greeting)

	userJSONRead, _ := db.GetJSON("user:alice")
	var userRead map[string]interface{}
	json.Unmarshal(userJSONRead, &userRead)
	fmt.Printf("  user:alice = %v\n", userRead)

	fmt.Println()
	fmt.Println("📊 Database statistics:")
	stats := db.Stats()
	fmt.Printf("  File size: %d bytes\n", stats.FileBytes)
	fmt.Printf("  Keys: %d\n", stats.Keys)
	fmt.Printf("  Live records: %d\n", stats.LiveRecords)
	fmt.Printf("  Last compacted: %s\n", time.Unix(stats.LastCompactedUnix, 0).Format(time.RFC3339))

	fmt.Println()
	fmt.Println("🗑️  Deleting a key...")
	if err := db.Delete("user:count"); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Println("  ✓ Deleted user:count")

	// Try to read deleted key
	_, err = db.GetInt("user:count")
	if err == solodb.ErrNotFound {
		fmt.Println("  ✓ Confirmed: key not found after deletion")
	}

	fmt.Println()
	fmt.Println("✅ Example completed successfully!")
	fmt.Println()
	fmt.Println("Note: Database is automatically compacted on next Open()")
}
