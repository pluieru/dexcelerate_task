package agg

import (
	"dexc_conf/internal/models"
	"dexc_conf/internal/store"
	"sync"
	"time"
)

type RingBuffer struct {
	mu          sync.RWMutex
	buckets     []store.Bucket
	bucketIndex map[time.Time]int
	capacity    int
	head        int
	size        int
	bucketStep  time.Duration
	window      models.Window
}

// NewRingBuffer creates a new ring buffer
func NewRingBuffer(window models.Window, bucketStep time.Duration) (*RingBuffer, error) {

	windowDuration, err := window.Duration()
	if err != nil {
		return nil, err
	}

	capacity := int(windowDuration / bucketStep)
	if capacity <= 0 {
		capacity = 1
	}

	return &RingBuffer{
		buckets:     make([]store.Bucket, capacity),
		bucketIndex: make(map[time.Time]int),
		capacity:    capacity,
		head:        0,
		size:        0,
		bucketStep:  bucketStep,
		window:      window,
	}, nil
}

// Add adds values to the bucket for the given time
// Returns the buckets that have expired (for subtraction from the aggregate)
func (rb *RingBuffer) Add(bucketStart time.Time, volUSD float64, txCount int64) []store.Bucket {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// find the bucket for the given time
	index := rb.findOrCreateBucket(bucketStart)

	// add the values to the bucket
	rb.buckets[index].Add(volUSD, txCount)

	// return the buckets that have expired
	return rb.evictOldBuckets(bucketStart)
}

// findOrCreateBucket finds or creates a bucket for the given time
func (rb *RingBuffer) findOrCreateBucket(bucketStart time.Time) int {
	// fast search in map
	if idx, exists := rb.bucketIndex[bucketStart]; exists {
		return idx
	}

	// if the buffer is empty, create the first bucket
	if rb.size == 0 {
		rb.buckets[0] = store.Bucket{
			Start:   bucketStart,
			VolUSD:  0,
			TxCount: 0,
		}
		rb.bucketIndex[bucketStart] = 0
		rb.head = 0
		rb.size = 1
		return 0
	}

	// if not found, create a new bucket
	return rb.createNewBucket(bucketStart)
}

// createNewBucket creates a new bucket, possibly evicting old ones
func (rb *RingBuffer) createNewBucket(bucketStart time.Time) int {
	if rb.size < rb.capacity {
		// there is space, just add
		idx := (rb.head + rb.size) % rb.capacity
		rb.buckets[idx] = store.Bucket{
			Start:   bucketStart,
			VolUSD:  0,
			TxCount: 0,
		}
		rb.bucketIndex[bucketStart] = idx
		rb.size++
		return idx
	}

	// buffer is full, replace the oldest
	idx := rb.head

	// remove the oldest bucket from the index
	oldBucket := rb.buckets[idx]
	if !oldBucket.Start.IsZero() {
		delete(rb.bucketIndex, oldBucket.Start)
	}

	// create a new bucket
	rb.buckets[idx] = store.Bucket{
		Start:   bucketStart,
		VolUSD:  0,
		TxCount: 0,
	}
	rb.bucketIndex[bucketStart] = idx
	rb.head = (rb.head + 1) % rb.capacity
	return idx
}

// evictOldBuckets evicts buckets that have expired
func (rb *RingBuffer) evictOldBuckets(currentTime time.Time) []store.Bucket {
	windowDuration, _ := rb.window.Duration()
	cutoffTime := currentTime.Add(-windowDuration)

	var evicted []store.Bucket

	// remove old buckets
	for rb.size > 0 {
		headIdx := rb.head
		bucket := rb.buckets[headIdx]
		if bucket.Start.Before(cutoffTime) {
				// save the bucket to be evicted
			evicted = append(evicted, bucket)

			// remove from index
			if !bucket.Start.IsZero() {
				delete(rb.bucketIndex, bucket.Start)
			}

			// clear the bucket
			rb.buckets[headIdx] = store.Bucket{}

			// shift the head
			rb.head = (rb.head + 1) % rb.capacity
			rb.size--
		} else {
			break
		}
	}

	return evicted
}

// GetAggregate calculates the aggregate for all active buckets
func (rb *RingBuffer) GetAggregate() models.Aggregate {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	var agg models.Aggregate

	for i := 0; i < rb.size; i++ {
		idx := (rb.head + i) % rb.capacity
		bucket := rb.buckets[idx]
		agg.Add(bucket.VolUSD, bucket.TxCount)
	}

	return agg
}

// GetBuckets returns all active buckets
func (rb *RingBuffer) GetBuckets() []store.Bucket {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	buckets := make([]store.Bucket, rb.size)
	for i := 0; i < rb.size; i++ {
		idx := (rb.head + i) % rb.capacity
		buckets[i] = rb.buckets[idx]
	}

	return buckets
}

// LoadBuckets loads buckets into the ring buffer (for recovery)
func (rb *RingBuffer) LoadBuckets(buckets []store.Bucket) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// clear existing data
	rb.head = 0
	rb.size = 0
	rb.bucketIndex = make(map[time.Time]int)
	for i := range rb.buckets {
		rb.buckets[i] = store.Bucket{}
	}

	// load new buckets
	for _, bucket := range buckets {
		if rb.size < rb.capacity {
			idx := (rb.head + rb.size) % rb.capacity
			rb.buckets[idx] = bucket
			rb.bucketIndex[bucket.Start] = idx
			rb.size++
		}
	}
}

// Clear clears all buckets
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.head = 0
	rb.size = 0
	rb.bucketIndex = make(map[time.Time]int)
	for i := range rb.buckets {
		rb.buckets[i] = store.Bucket{}
	}
}

// Size returns the number of active buckets
func (rb *RingBuffer) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

// Capacity returns the maximum capacity of the buffer
func (rb *RingBuffer) Capacity() int {
	return rb.capacity
}
