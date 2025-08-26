package agg

import (
	"dexc_conf/internal/models"
	"sync"
	"time"
)

// ReorderBuffer buffers events for out-of-order processing
type ReorderBuffer struct {
	mu           sync.RWMutex
	buffer       []models.Swap // buffer of events
	watermark    time.Time     // current watermark
	delay        time.Duration // delay for reordering
	maxWatermark time.Time     // maximum timestamp we have seen
}

// NewReorderBuffer creates a new buffer for reordering
func NewReorderBuffer(delay time.Duration) *ReorderBuffer {
	return &ReorderBuffer{
		buffer: make([]models.Swap, 0),
		delay:  delay,
	}
}

// Add adds an event to the buffer
// Returns the list of events ready for processing
func (rb *ReorderBuffer) Add(swap models.Swap) ([]models.Swap, []models.Swap) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// update the maximum timestamp
	if swap.TS.After(rb.maxWatermark) {
		rb.maxWatermark = swap.TS
	}

	// calculate the new watermark
	newWatermark := rb.maxWatermark.Add(-rb.delay)

			// if the event is too old (before watermark), discard it
	if swap.TS.Before(rb.watermark) {
		return nil, []models.Swap{swap} // return as dropped
	}

	// add the event to the buffer
	rb.buffer = append(rb.buffer, swap)

	// update the watermark
	rb.watermark = newWatermark

	// extract ready events
	ready, remaining := rb.extractReady()
	rb.buffer = remaining

	return ready, nil
}

// extractReady extracts events ready for processing (before watermark)
func (rb *ReorderBuffer) extractReady() ([]models.Swap, []models.Swap) {
	ready := make([]models.Swap, 0)
	remaining := make([]models.Swap, 0)

	for _, swap := range rb.buffer {
		if swap.TS.Before(rb.watermark) || swap.TS.Equal(rb.watermark) {
			ready = append(ready, swap)
		} else {
			remaining = append(remaining, swap)
		}
	}

	return ready, remaining
}

// Flush forcibly extracts all events from the buffer
// Used at shutdown
func (rb *ReorderBuffer) Flush() []models.Swap {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	ready := make([]models.Swap, len(rb.buffer))
	copy(ready, rb.buffer)
	rb.buffer = rb.buffer[:0] // clear the buffer

	return ready
}

// GetWatermark returns the current watermark
func (rb *ReorderBuffer) GetWatermark() time.Time {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.watermark
}

// SetWatermark sets the watermark (used for recovery)
func (rb *ReorderBuffer) SetWatermark(watermark time.Time) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.watermark = watermark
	if watermark.After(rb.maxWatermark) {
		rb.maxWatermark = watermark
	}
}

// Size returns the current size of the buffer
func (rb *ReorderBuffer) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return len(rb.buffer)
}

// PeriodicFlush periodically extracts ready events
// Used to process events that have been in the buffer for a long time
func (rb *ReorderBuffer) PeriodicFlush(interval time.Duration) []models.Swap {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// force the watermark to be interval behind the current time
	now := time.Now()
	forceWatermark := now.Add(-rb.delay)

	if forceWatermark.After(rb.watermark) {
		rb.watermark = forceWatermark

		// extract ready events
		ready, remaining := rb.extractReady()
		rb.buffer = remaining
		return ready
	}

	return nil
}
