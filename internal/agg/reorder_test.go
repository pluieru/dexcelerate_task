package agg

import (
	"dexc_conf/internal/models"
	"testing"
	"time"
)

func createTestSwap(id string, ts time.Time) models.Swap {
	return models.Swap{
		ID:       id,
		Who:      "0x123",
		Token:    "WETH",
		Amount:   100.0,
		USD:      1500.0,
		Side:     models.SideBuy,
		TS:       ts,
		TxHash:   "0xabc",
		LogIndex: 1,
	}
}

func TestReorderBuffer_InOrder(t *testing.T) {
	delay := 3 * time.Second
	buffer := NewReorderBuffer(delay)

	now := time.Now()

	// send events in order
	swap1 := createTestSwap("1", now)
	swap2 := createTestSwap("2", now.Add(1*time.Second))
	swap3 := createTestSwap("3", now.Add(2*time.Second))

	// first event
	ready, dropped := buffer.Add(swap1)
	if len(ready) != 0 {
		t.Errorf("Expected 0 ready events, got %d", len(ready))
	}
	if len(dropped) != 0 {
		t.Errorf("Expected 0 dropped events, got %d", len(dropped))
	}

	// second event
	ready, dropped = buffer.Add(swap2)
	if len(ready) != 0 {
		t.Errorf("Expected 0 ready events, got %d", len(ready))
	}
	if len(dropped) != 0 {
		t.Errorf("Expected 0 dropped events, got %d", len(dropped))
	}

	// third event - this will move the watermark but not necessarily release events
	_, dropped = buffer.Add(swap3)
	if len(dropped) != 0 {
		t.Errorf("Expected 0 dropped events, got %d", len(dropped))
	}

	// add a future event to move the watermark far enough
	futureSwap := createTestSwap("future", now.Add(10*time.Second))
	ready, dropped = buffer.Add(futureSwap)

	// Теперь должны быть готовые события
	if len(ready) < 1 {
		t.Errorf("Expected at least 1 ready event, got %d", len(ready))
	}
	if len(dropped) != 0 {
		t.Errorf("Expected 0 dropped events, got %d", len(dropped))
	}
}

func TestReorderBuffer_OutOfOrder(t *testing.T) {
	delay := 3 * time.Second
	buffer := NewReorderBuffer(delay)

	now := time.Now()

	// send events in the wrong order
	swap3 := createTestSwap("3", now.Add(3*time.Second))
	swap1 := createTestSwap("1", now.Add(1*time.Second))
	swap2 := createTestSwap("2", now.Add(2*time.Second))

	// first event (time 3s)
	ready, dropped := buffer.Add(swap3)
	if len(ready) != 0 {
		t.Errorf("Expected 0 ready events, got %d", len(ready))
	}

	// second event (time 1s) - out of order, but within the delay
	ready, dropped = buffer.Add(swap1)
	if len(ready) != 0 {
		t.Errorf("Expected 0 ready events, got %d", len(ready))
	}
	if len(dropped) != 0 {
		t.Errorf("Expected 0 dropped events, got %d", len(dropped))
	}

	// third event (time 2s) - out of order, but within the delay
	ready, dropped = buffer.Add(swap2)
	if len(ready) != 0 {
		t.Errorf("Expected 0 ready events, got %d", len(ready))
	}
	if len(dropped) != 0 {
		t.Errorf("Expected 0 dropped events, got %d", len(dropped))
	}

	// check the buffer size
	if size := buffer.Size(); size != 3 {
		t.Errorf("Expected buffer size 3, got %d", size)
	}
}

func TestReorderBuffer_DroppedEvents(t *testing.T) {
	delay := 3 * time.Second
	buffer := NewReorderBuffer(delay)

	now := time.Now()

	// first send a recent event to set the watermark
	recentSwap := createTestSwap("recent", now.Add(10*time.Second))
	buffer.Add(recentSwap)

		// now send an old event (before watermark)
	oldSwap := createTestSwap("old", now.Add(1*time.Second))
	ready, dropped := buffer.Add(oldSwap)

	if len(ready) != 0 {
		t.Errorf("Expected 0 ready events, got %d", len(ready))
	}
	if len(dropped) != 1 {
		t.Errorf("Expected 1 dropped event, got %d", len(dropped))
	}
	if dropped[0].ID != "old" {
		t.Errorf("Expected dropped event ID 'old', got '%s'", dropped[0].ID)
	}
}

func TestReorderBuffer_Watermark(t *testing.T) {
	delay := 3 * time.Second
	buffer := NewReorderBuffer(delay)

	now := time.Now()

	// send an event
	swap := createTestSwap("1", now)
	buffer.Add(swap)

	// check that the watermark is set
	watermark := buffer.GetWatermark()
	expected := now.Add(-delay)

			// allow for some error due to execution time
	if watermark.Before(expected.Add(-100*time.Millisecond)) ||
		watermark.After(expected.Add(100*time.Millisecond)) {
		t.Errorf("Expected watermark around %v, got %v", expected, watermark)
	}
}

func TestReorderBuffer_SetWatermark(t *testing.T) {
	buffer := NewReorderBuffer(3 * time.Second)

	testTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	buffer.SetWatermark(testTime)

	if watermark := buffer.GetWatermark(); !watermark.Equal(testTime) {
		t.Errorf("Expected watermark %v, got %v", testTime, watermark)
	}
}

func TestReorderBuffer_Flush(t *testing.T) {
	buffer := NewReorderBuffer(3 * time.Second)

	now := time.Now()

	// add several events
	swap1 := createTestSwap("1", now)
	swap2 := createTestSwap("2", now.Add(1*time.Second))
	swap3 := createTestSwap("3", now.Add(2*time.Second))

	buffer.Add(swap1)
	buffer.Add(swap2)
	buffer.Add(swap3)

	// check the buffer size
	if size := buffer.Size(); size != 3 {
		t.Errorf("Expected buffer size 3 before flush, got %d", size)
	}

	// perform flush
	flushed := buffer.Flush()

	// check that all events were returned
	if len(flushed) != 3 {
		t.Errorf("Expected 3 flushed events, got %d", len(flushed))
	}

	// check that the buffer is empty
	if size := buffer.Size(); size != 0 {
		t.Errorf("Expected buffer size 0 after flush, got %d", size)
	}
}

func TestReorderBuffer_PeriodicFlush(t *testing.T) {
	delay := 1 * time.Second
	buffer := NewReorderBuffer(delay)

	now := time.Now()

	// add an old event
	oldSwap := createTestSwap("old", now.Add(-2*time.Second))
	buffer.Add(oldSwap)

	// wait a bit
	time.Sleep(10 * time.Millisecond)

	// perform periodic flush
	flushed := buffer.PeriodicFlush(100 * time.Millisecond)

	// should return the old event
	if len(flushed) != 1 {
		t.Errorf("Expected 1 flushed event, got %d", len(flushed))
	}
	if len(flushed) > 0 && flushed[0].ID != "old" {
		t.Errorf("Expected flushed event ID 'old', got '%s'", flushed[0].ID)
	}
}

func TestReorderBuffer_Size(t *testing.T) {
	buffer := NewReorderBuffer(3 * time.Second)

	now := time.Now()
	swap := createTestSwap("1", now)
	buffer.Add(swap)

	if buffer.Size() != 1 {
		t.Errorf("Expected buffer size 1, got %d", buffer.Size())
	}
}

func BenchmarkReorderBuffer_Add(b *testing.B) {
	buffer := NewReorderBuffer(3 * time.Second)
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		swap := createTestSwap("bench", now.Add(time.Duration(i)*time.Millisecond))
		buffer.Add(swap)
	}
}
