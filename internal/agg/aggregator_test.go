package agg

import (
	"context"
	"dexc_conf/internal/models"
	"testing"
	"time"
)

// helper function to process events through reorder buffer
func processEventsWithReorder(t *testing.T, agg Aggregator, swaps []models.Swap) {
	ctx := context.Background()

	// send all events
	for _, swap := range swaps {
		agg.Ingest(ctx, swap)
	}

	// send an event from far in the future to move the watermark and process all events
	futureSwap := models.Swap{
		ID:       "watermark_pusher",
		Who:      "0x999",
		Token:    "WATERMARK",
		Amount:   1.0,
		USD:      1.0,
		Side:     models.SideBuy,
		TS:       time.Now().Add(time.Hour), // far in the future
		TxHash:   "0xwatermark",
		LogIndex: 1,
	}
	agg.Ingest(ctx, futureSwap)

	// small pause for processing
	time.Sleep(50 * time.Millisecond)
}

func TestAggregator_Ingest(t *testing.T) {
	windows := []models.Window{models.Window5m, models.Window1h}
	bucketStep := 5 * time.Second
	dedupTTL := time.Hour
	reorderDelay := 10 * time.Millisecond // short delay for test

	agg, err := NewAggregator(windows, bucketStep, dedupTTL, reorderDelay, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	defer agg.Close()

	ctx := context.Background()
	// use old time so it exactly passes through the reorder buffer
	now := time.Now().Add(-time.Minute)

	// create a test swap
	swap := models.Swap{
		ID:       "test_1",
		Who:      "0x123",
		Token:    "WETH",
		Amount:   100.0,
		USD:      1500.0,
		Side:     models.SideBuy,
		TS:       now,
		TxHash:   "0xabc",
		LogIndex: 1,
	}

	// process the event
	agg.Ingest(ctx, swap)

	// send a new event to move the watermark
	futureSwap := models.Swap{
		ID:       "test_future",
		Who:      "0x123",
		Token:    "USDC",
		Amount:   1.0,
		USD:      1.0,
		Side:     models.SideBuy,
		TS:       time.Now(),
		TxHash:   "0xdef",
		LogIndex: 1,
	}
	agg.Ingest(ctx, futureSwap)

	// wait for processing
	time.Sleep(100 * time.Millisecond)

	// get statistics
	aggregates := agg.Get("WETH")

	// check that the values are correct
	if aggregates.W5m.VolUSD != 1500.0 {
		t.Errorf("Expected 5m volume 1500.0, got %f", aggregates.W5m.VolUSD)
	}
	if aggregates.W5m.TxCount != 1 {
		t.Errorf("Expected 5m tx count 1, got %d", aggregates.W5m.TxCount)
	}
	if aggregates.W1h.VolUSD != 1500.0 {
		t.Errorf("Expected 1h volume 1500.0, got %f", aggregates.W1h.VolUSD)
	}
	if aggregates.W1h.TxCount != 1 {
		t.Errorf("Expected 1h tx count 1, got %d", aggregates.W1h.TxCount)
	}
}

func TestAggregator_Deduplication(t *testing.T) {
	windows := []models.Window{models.Window5m}
	bucketStep := 5 * time.Second
	dedupTTL := time.Hour
	reorderDelay := 100 * time.Millisecond // short delay for test

	agg, err := NewAggregator(windows, bucketStep, dedupTTL, reorderDelay, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	defer agg.Close()

		// use old time so it exactly passes through the reorder buffer
	now := time.Now().Add(-time.Minute)

	// create a swap twice
	swap := models.Swap{
		ID:       "duplicate_test",
		Who:      "0x123",
		Token:    "WETH",
		Amount:   100.0,
		USD:      1500.0,
		Side:     models.SideBuy,
		TS:       now,
		TxHash:   "0xabc",
		LogIndex: 1,
	}

	// process events with helper function
	processEventsWithReorder(t, agg, []models.Swap{swap, swap}) // twice the same

	// get statistics
	aggregates := agg.Get("WETH")

	// should be counted only once
	if aggregates.W5m.VolUSD != 1500.0 {
		t.Errorf("Expected volume 1500.0 (no duplicates), got %f", aggregates.W5m.VolUSD)
	}
	if aggregates.W5m.TxCount != 1 {
		t.Errorf("Expected tx count 1 (no duplicates), got %d", aggregates.W5m.TxCount)
	}
}

func TestAggregator_MultipleTokens(t *testing.T) {
	windows := []models.Window{models.Window5m}
	bucketStep := 5 * time.Second
	dedupTTL := time.Hour
	reorderDelay := 100 * time.Millisecond

	agg, err := NewAggregator(windows, bucketStep, dedupTTL, reorderDelay, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	defer agg.Close()

	// use old time so it exactly passes through the reorder buffer
	now := time.Now().Add(-time.Minute)

	// create swaps for different tokens
	swaps := []models.Swap{
		{
			ID: "weth_1", Who: "0x123", Token: "WETH", Amount: 100.0, USD: 1500.0,
			Side: models.SideBuy, TS: now, TxHash: "0xabc1", LogIndex: 1,
		},
		{
			ID: "usdc_1", Who: "0x123", Token: "USDC", Amount: 1000.0, USD: 1000.0,
			Side: models.SideSell, TS: now, TxHash: "0xabc2", LogIndex: 1,
		},
		{
			ID: "weth_2", Who: "0x123", Token: "WETH", Amount: 50.0, USD: 750.0,
			Side: models.SideBuy, TS: now, TxHash: "0xabc3", LogIndex: 1,
		},
	}

	// process events with helper function
	processEventsWithReorder(t, agg, swaps)

	// get statistics for all tokens
	allStats := agg.GetAll()

	// check WETH (2 transactions)
	wethStats, exists := allStats["WETH"]
	if !exists {
		t.Fatal("WETH stats not found")
	}
	if wethStats.W5m.VolUSD != 2250.0 { // 1500 + 750
		t.Errorf("Expected WETH volume 2250.0, got %f", wethStats.W5m.VolUSD)
	}
	if wethStats.W5m.TxCount != 2 {
		t.Errorf("Expected WETH tx count 2, got %d", wethStats.W5m.TxCount)
	}

	// check USDC (1 transaction)
	usdcStats, exists := allStats["USDC"]
	if !exists {
		t.Fatal("USDC stats not found")
	}
	if usdcStats.W5m.VolUSD != 1000.0 {
		t.Errorf("Expected USDC volume 1000.0, got %f", usdcStats.W5m.VolUSD)
	}
	if usdcStats.W5m.TxCount != 1 {
		t.Errorf("Expected USDC tx count 1, got %d", usdcStats.W5m.TxCount)
	}

	// check the total number of tokens (exclude WATERMARK token from helper function)
	realTokenCount := 0
	for token := range allStats {
		if token != "WATERMARK" {
			realTokenCount++
		}
	}
	if realTokenCount != 2 {
		t.Errorf("Expected 2 real tokens, got %d", realTokenCount)
	}
}

func TestAggregator_BucketAlignment(t *testing.T) {
	windows := []models.Window{models.Window5m}
	bucketStep := 5 * time.Second
	dedupTTL := time.Hour
	reorderDelay := 100 * time.Millisecond

	agg, err := NewAggregator(windows, bucketStep, dedupTTL, reorderDelay, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	defer agg.Close()

	// create base time aligned to 5 seconds in the past
	baseTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	// create events in the same bucket (spread within 5 seconds)
	swaps := []models.Swap{
		{
			ID: "1", Who: "0x123", Token: "WETH", Amount: 100.0, USD: 1000.0,
			Side: models.SideBuy, TS: baseTime, TxHash: "0x1", LogIndex: 1,
		},
		{
			ID: "2", Who: "0x123", Token: "WETH", Amount: 100.0, USD: 1000.0,
			Side: models.SideBuy, TS: baseTime.Add(2 * time.Second), TxHash: "0x2", LogIndex: 1,
		},
		{
			ID: "3", Who: "0x123", Token: "WETH", Amount: 100.0, USD: 1000.0,
			Side: models.SideBuy, TS: baseTime.Add(4 * time.Second), TxHash: "0x3", LogIndex: 1,
		},
	}

		// process events with helper function
	processEventsWithReorder(t, agg, swaps)

	// all events should be in the same bucket
	aggregates := agg.Get("WETH")
	if aggregates.W5m.VolUSD != 3000.0 {
		t.Errorf("Expected volume 3000.0 (all in same bucket), got %f", aggregates.W5m.VolUSD)
	}
	if aggregates.W5m.TxCount != 3 {
		t.Errorf("Expected tx count 3 (all in same bucket), got %d", aggregates.W5m.TxCount)
	}
}

func TestAggregator_SnapshotRestore(t *testing.T) {
	windows := []models.Window{models.Window5m}
	bucketStep := 5 * time.Second
	dedupTTL := time.Hour
	reorderDelay := 100 * time.Millisecond

	// create the first aggregator
	agg1, err := NewAggregator(windows, bucketStep, dedupTTL, reorderDelay, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	defer agg1.Close()

	// use old time so it exactly passes through the reorder buffer
	now := time.Now().Add(-time.Minute)

	// add data
	swap := models.Swap{
		ID: "snapshot_test", Who: "0x123", Token: "WETH", Amount: 100.0, USD: 1500.0,
		Side: models.SideBuy, TS: now, TxHash: "0xabc", LogIndex: 1,
	}

	// process the event with helper function
	processEventsWithReorder(t, agg1, []models.Swap{swap})

	// create a snapshot
	snapshot := agg1.Snapshot()

	// create the second aggregator with separate metrics
	agg2, err := NewAggregator(windows, bucketStep, dedupTTL, reorderDelay, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create second aggregator: %v", err)
	}
	defer agg2.Close()

	// restore from snapshot
	err = agg2.Restore(snapshot)
	if err != nil {
		t.Fatalf("Failed to restore snapshot: %v", err)
	}

	// check that the data was restored
	aggregates := agg2.Get("WETH")
	if aggregates.W5m.VolUSD != 1500.0 {
		t.Errorf("Expected volume 1500.0 after restore, got %f", aggregates.W5m.VolUSD)
	}
	if aggregates.W5m.TxCount != 1 {
		t.Errorf("Expected tx count 1 after restore, got %d", aggregates.W5m.TxCount)
	}
}

func TestAggregator_InvalidSwap(t *testing.T) {
	windows := []models.Window{models.Window5m}
	bucketStep := 5 * time.Second
	dedupTTL := time.Hour
	reorderDelay := 100 * time.Millisecond

	agg, err := NewAggregator(windows, bucketStep, dedupTTL, reorderDelay, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	defer agg.Close()

	ctx := context.Background()

	// create an invalid swap (no token)
	invalidSwap := models.Swap{
		ID:       "invalid",
		Who:      "0x123",
		Token:    "", // empty token = invalid
		Amount:   100.0,
		USD:      1500.0,
		Side:     models.SideBuy,
		TS:       time.Now(),
		TxHash:   "0xabc",
		LogIndex: 1,
	}

	// process the invalid swap
	agg.Ingest(ctx, invalidSwap)
	time.Sleep(200 * time.Millisecond)

			// check that the statistics are empty
	allStats := agg.GetAll()
	if len(allStats) != 0 {
		t.Errorf("Expected no stats for invalid swap, got %d tokens", len(allStats))
	}
}

func BenchmarkAggregator_Ingest(b *testing.B) {
	windows := []models.Window{models.Window5m, models.Window1h}
	bucketStep := 5 * time.Second
	dedupTTL := time.Hour
	reorderDelay := 3 * time.Second

	agg, err := NewAggregator(windows, bucketStep, dedupTTL, reorderDelay, 5*time.Minute)
	if err != nil {
		b.Fatalf("Failed to create aggregator: %v", err)
	}
	defer agg.Close()

	ctx := context.Background()
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		swap := models.Swap{
			ID: "bench_" + string(rune(i)), Who: "0x123", Token: "WETH", Amount: 100.0, USD: 1500.0,
			Side: models.SideBuy, TS: now.Add(time.Duration(i) * time.Millisecond), TxHash: "0xabc", LogIndex: 1,
		}
		agg.Ingest(ctx, swap)
	}
}
