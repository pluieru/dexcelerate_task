package agg

import (
	"sync"
	"time"
)

// DedupEntry represents an entry in the deduplicator
type DedupEntry struct {
	Key       string    `json:"key"`        // event key
	ExpiresAt time.Time `json:"expires_at"` // expiration time
}

// Deduplicator provides deduplication of events with TTL
type Deduplicator struct {
	mu      sync.RWMutex
	entries map[string]time.Time // key -> expiration time
	ttl     time.Duration        // время жизни записи
}

func NewDeduplicator(ttl time.Duration) *Deduplicator {
	return &Deduplicator{
		entries: make(map[string]time.Time),
		ttl:     ttl,
	}
}

// IsDuplicate checks if an event is a duplicate
// If the event is new, adds it to the set and returns false
// If the event already exists, returns true
func (d *Deduplicator) IsDuplicate(key string) bool {
	now := time.Now()

	d.mu.Lock()
	defer d.mu.Unlock()

	// check if the key exists
	if expTime, exists := d.entries[key]; exists {
		// if the entry is not expired, it is a duplicate
		if now.Before(expTime) {
			return true
		}
		// if expired, delete the old entry
		delete(d.entries, key)
	}

	// add the new entry
	d.entries[key] = now.Add(d.ttl)
	return false
}

// Cleanup clears expired entries
// Should be called periodically to free memory
func (d *Deduplicator) Cleanup() int {
	now := time.Now()
	cleaned := 0

	d.mu.Lock()
	defer d.mu.Unlock()

	for key, expTime := range d.entries {
		if now.After(expTime) {
			delete(d.entries, key)
			cleaned++
		}
	}

	return cleaned
}

// Size returns the current number of entries
func (d *Deduplicator) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.entries)
}

// Clear clears all entries
func (d *Deduplicator) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries = make(map[string]time.Time)
}

// StartCleanupRoutine starts a routine for periodic cleanup
func (d *Deduplicator) StartCleanupRoutine(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cleaned := d.Cleanup()
			if cleaned > 0 {
						// can add logging
				_ = cleaned
			}
		case <-stopCh:
			return
		}
	}
}
