package main

import (
	"fmt"
	"log"
	"time"

	solodb "github.com/phillarmonic/SoloDB"
)

func main() {
	dbPath := "expiration_test.solo"

	fmt.Println("🎵 SoloDB - Expiration Demo")
	fmt.Println("=" + string(make([]byte, 50)))
	fmt.Println()

	// Open database initially
	db, err := solodb.Open(solodb.Options{
		Path:       dbPath,
		Durability: solodb.SyncAlways,
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Set expiration to 5 seconds from now
	expirationTime := time.Now().Add(5 * time.Second)
	fmt.Printf("⏰ Setting key 'session:demo' with expiration at: %s\n", expirationTime.Format("15:04:05"))
	fmt.Printf("   (expires in 5 seconds)\n\n")

	if err := db.SetString("session:demo", "active-session-data", expirationTime); err != nil {
		log.Fatalf("Failed to set value: %v", err)
	}

	// Also set a non-expiring key for comparison
	noExpire := time.Unix(9223372036854775807, 0) // math.MaxInt64
	if err := db.SetString("persistent:key", "never-expires", noExpire); err != nil {
		log.Fatalf("Failed to set persistent value: %v", err)
	}

	stats := db.Stats()
	fmt.Printf("📊 Initial stats: %d keys, %d bytes, %d live records\n",
		stats.Keys, stats.FileBytes, stats.LiveRecords)
	fmt.Println("   Note: Lazy GC will automatically clean expired keys from index on read")

	// Query loop - check every 500ms for 10 seconds
	fmt.Println("🔍 Querying every 500ms until expiration...")
	fmt.Println()

	startTime := time.Now()
	queryCount := 0
	maxDuration := 10 * time.Second

	for time.Since(startTime) < maxDuration {
		queryCount++
		now := time.Now()
		elapsed := now.Sub(startTime)

		// Try to get the expiring key
		value, err := db.GetString("session:demo")

		timeUntilExpiry := expirationTime.Sub(now)

		if err == nil {
			// Key still valid
			fmt.Printf("  [%6.1fs] Query #%2d: ✅ Found (value: '%s', expires in %.1fs)\n",
				elapsed.Seconds(), queryCount, value, timeUntilExpiry.Seconds())
		} else if err == solodb.ErrExpired {
			// Key expired!
			fmt.Printf("  [%6.1fs] Query #%2d: ⏱️  EXPIRED (%.1fs past expiration)\n",
				elapsed.Seconds(), queryCount, -timeUntilExpiry.Seconds())
			fmt.Println()
			fmt.Println("🎯 Expiration detected! Lazy GC automatically removed it from index.")
			break
		} else if err == solodb.ErrNotFound {
			// Key not found (after GC)
			fmt.Printf("  [%6.1fs] Query #%2d: ❌ NOT FOUND (removed by GC)\n",
				elapsed.Seconds(), queryCount)
			fmt.Println()
			fmt.Println("🗑️  Key completely removed by garbage collection.")
			break
		} else {
			log.Fatalf("Unexpected error: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
	}

	// Check persistent key is still there
	fmt.Println()
	fmt.Println("🔍 Checking persistent key...")
	persistentValue, err := db.GetString("persistent:key")
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Still exists: '%s'\n", persistentValue)
	}

	statsBeforeCompact := db.Stats()
	fmt.Printf("\n📊 Before manual compaction: %d keys, %d bytes, %d live records\n",
		statsBeforeCompact.Keys, statsBeforeCompact.FileBytes, statsBeforeCompact.LiveRecords)

	// Manually trigger compaction to reclaim disk space
	fmt.Println()
	fmt.Println("🔄 Running manual compaction (db.Compact() to reclaim disk space)...")
	if err := db.Compact(); err != nil {
		log.Fatalf("Failed to compact database: %v", err)
	}

	statsAfterCompact := db.Stats()
	fmt.Printf("📊 After compaction: %d keys, %d bytes, %d live records\n",
		statsAfterCompact.Keys, statsAfterCompact.FileBytes, statsAfterCompact.LiveRecords)

	fmt.Println()
	fmt.Printf("💾 File size reduced by %d bytes\n",
		statsBeforeCompact.FileBytes-statsAfterCompact.FileBytes)

	// Final verification
	fmt.Println()
	fmt.Println("🔍 Final verification after compaction...")

	_, err = db.GetString("session:demo")
	if err == solodb.ErrNotFound {
		fmt.Println("   ✅ Expired key successfully removed from database")
	} else if err == solodb.ErrExpired {
		fmt.Println("   ⚠️  Key still marked as expired (shouldn't happen after compaction)")
	} else {
		fmt.Printf("   ❌ Unexpected result: %v\n", err)
	}

	persistentValue, err = db.GetString("persistent:key")
	if err != nil {
		fmt.Printf("   ❌ Persistent key error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Persistent key still exists: '%s'\n", persistentValue)
	}

	fmt.Println()
	fmt.Println("✅ Expiration demo completed!")
	fmt.Println()
	fmt.Println("Key findings:")
	fmt.Println("  • Keys expire at their specified time")
	fmt.Println("  • Expired keys return ErrExpired on read")
	fmt.Println("  • Lazy GC automatically removes expired keys from index on read")
	fmt.Println("  • Manual db.Compact() reclaims disk space (no restart needed)")
	fmt.Println("  • Non-expiring keys persist through all GC cycles")
}
