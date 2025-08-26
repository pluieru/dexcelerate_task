package agg

import (
	"testing"
	"time"
)

func TestDeduplicator_IsDuplicate(t *testing.T) {
	tests := []struct {
		name       string
		ttl        time.Duration
		operations []struct {
			key      string
			delay    time.Duration
			expected bool
		}
	}{
		{
			name: "simple duplicate detection",
			ttl:  time.Hour,
			operations: []struct {
				key      string
				delay    time.Duration
				expected bool
			}{
				{"key1", 0, false}, // first appearance
				{"key1", 0, true},  // duplicate
				{"key2", 0, false}, // new key
				{"key1", 0, true},  // still a duplicate
				{"key2", 0, true},  // duplicate key2
			},
		},
		{
			name: "TTL expiration",
			ttl:  100 * time.Millisecond,
			operations: []struct {
				key      string
				delay    time.Duration
				expected bool
			}{
				{"key1", 0, false},                      // first appearance
				{"key1", 50 * time.Millisecond, true},   // duplicate (TTL not expired)
				{"key1", 100 * time.Millisecond, false}, // TTL expired, not a duplicate
				{"key1", 0, true},                       // still a duplicate
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dedup := NewDeduplicator(tt.ttl)

			for i, op := range tt.operations {
				if op.delay > 0 {
					time.Sleep(op.delay)
				}

				got := dedup.IsDuplicate(op.key)
				if got != op.expected {
					t.Errorf("operation %d: IsDuplicate(%q) = %v, want %v", i, op.key, got, op.expected)
				}
			}
		})
	}
}

func TestDeduplicator_Cleanup(t *testing.T) {
	ttl := 50 * time.Millisecond
	dedup := NewDeduplicator(ttl)

	// add several keys
	dedup.IsDuplicate("key1")
	dedup.IsDuplicate("key2")
	dedup.IsDuplicate("key3")

	// check that all keys are present
	if size := dedup.Size(); size != 3 {
		t.Errorf("Expected size 3, got %d", size)
	}

	// wait for TTL to expire
	time.Sleep(ttl + 10*time.Millisecond)

	// add a new key (to activate cleanup in IsDuplicate)
	dedup.IsDuplicate("key4")

	// perform cleanup
	cleaned := dedup.Cleanup()

	// check that old keys were cleaned
	if cleaned < 3 {
		t.Errorf("Expected to clean at least 3 entries, cleaned %d", cleaned)
	}

	// check that only the new key remains
	if dedup.Size() > 1 {
		t.Errorf("Expected at most 1 active entry after cleanup, got %d", dedup.Size())
	}
}

func TestDeduplicator_Size(t *testing.T) {
	dedup := NewDeduplicator(time.Hour)

	// add keys
	keys := []string{"key1", "key2", "key3"}
	for _, key := range keys {
		dedup.IsDuplicate(key)
	}

	size := dedup.Size()
	if size != 3 {
		t.Errorf("Expected 3 entries, got %d", size)
	}
}

func TestDeduplicator_Clear(t *testing.T) {
	dedup := NewDeduplicator(time.Hour)

	// add keys
	dedup.IsDuplicate("key1")
	dedup.IsDuplicate("key2")

	if size := dedup.Size(); size != 2 {
		t.Errorf("Expected size 2 before clear, got %d", size)
	}

	// clear
	dedup.Clear()

	if size := dedup.Size(); size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", size)
	}

	// check that keys were actually removed
	if dedup.IsDuplicate("key1") {
		t.Error("key1 should not be duplicate after clear")
	}
}

func BenchmarkDeduplicator_IsDuplicate(b *testing.B) {
	dedup := NewDeduplicator(time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "benchmark_key"
		dedup.IsDuplicate(key)
	}
}

func BenchmarkDeduplicator_IsDuplicate_UniqueKeys(b *testing.B) {
	dedup := NewDeduplicator(time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key_" + string(rune(i))
		dedup.IsDuplicate(key)
	}
}

func TestDeduplicator_ConcurrentAccess(t *testing.T) {
	dedup := NewDeduplicator(time.Hour)

	// start several goroutines concurrently
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			key := "concurrent_key_" + string(rune(id))

				// each goroutine does many operations
			for j := 0; j < 100; j++ {
				dedup.IsDuplicate(key)
			}
		}(i)
	}

	// wait for all goroutines to finish
	for i := 0; i < 10; i++ {
		<-done
	}

	// check that the deduplicator is in a valid state
	if dedup.Size() != 10 {
		t.Errorf("Expected 10 unique entries, got %d", dedup.Size())
	}
}
