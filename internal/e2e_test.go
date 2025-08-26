package internal

import (
	"context"
	"dexc_conf/internal/agg"
	"dexc_conf/internal/httpapi"
	"dexc_conf/internal/ingest"
	"dexc_conf/internal/models"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"
)

// Helper function to process events and wait for aggregation
func processEventsForE2E(t *testing.T, aggregator agg.Aggregator, swaps []models.Swap) {
	ctx := context.Background()

	// Отправляем все события
	for _, swap := range swaps {
		aggregator.Ingest(ctx, swap)
	}

	// Отправляем событие из далекого будущего чтобы сдвинуть watermark
	futureSwap := models.Swap{
		ID:       "e2e_watermark_pusher",
		Who:      "0x999",
		Token:    "WATERMARK",
		Amount:   1.0,
		USD:      1.0,
		Side:     models.SideBuy,
		TS:       time.Now().Add(time.Hour),
		TxHash:   "0xwatermark",
		LogIndex: 1,
	}
	aggregator.Ingest(ctx, futureSwap)

	// Ждём обработки
	time.Sleep(100 * time.Millisecond)
}

func TestE2E_StatsEndpoint(t *testing.T) {
	// Создаём конфигурацию для тестов
	windows := []models.Window{models.Window5m, models.Window1h}
	bucketStep := 5 * time.Second
	dedupTTL := time.Hour
	reorderDelay := 100 * time.Millisecond

	// Создаём компоненты
	aggregator, err := agg.NewAggregator(windows, bucketStep, dedupTTL, reorderDelay, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	defer aggregator.Close()

	// Создаём HTTP роутер
	router := httpapi.NewRouter(aggregator)

	// Запускаем тестовый HTTP сервер на случайном порту
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	server := &http.Server{Handler: router}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			t.Logf("Server error: %v", err)
		}
	}()
	defer server.Shutdown(context.Background())

	baseURL := "http://" + listener.Addr().String()

	// Симулируем события (используем старое время)
	now := time.Now().Add(-time.Minute)

	testSwaps := []models.Swap{
		{
			ID: "e2e_weth_1", Who: "0x123", Token: "WETH", Amount: 100.0, USD: 1500.0,
			Side: models.SideBuy, TS: now, TxHash: "0x1", LogIndex: 1,
		},
		{
			ID: "e2e_weth_2", Who: "0x456", Token: "WETH", Amount: 200.0, USD: 3000.0,
			Side: models.SideSell, TS: now.Add(1 * time.Second), TxHash: "0x2", LogIndex: 1,
		},
		{
			ID: "e2e_usdc_1", Who: "0x789", Token: "USDC", Amount: 1000.0, USD: 1000.0,
			Side: models.SideBuy, TS: now.Add(2 * time.Second), TxHash: "0x3", LogIndex: 1,
		},
	}

	// Обрабатываем события с helper функцией
	processEventsForE2E(t, aggregator, testSwaps)

	// Проверяем что статистика доступна через HTTP API
	client := &http.Client{Timeout: 5 * time.Second}

	// Тестируем общую статистику
	resp, err := client.Get(baseURL + "/stats")
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var allStats httpapi.AllStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&allStats); err != nil {
		t.Fatalf("Failed to decode stats response: %v", err)
	}

	// Проверяем что у нас есть статистика по токенам (исключаем WATERMARK)
	realTokenCount := 0
	for _, stats := range allStats.Stats {
		if stats.Token != "WATERMARK" {
			realTokenCount++
		}
	}
	if realTokenCount < 2 {
		t.Errorf("Expected at least 2 real tokens, got %d", realTokenCount)
	}

	// Ищем статистику WETH
	var wethStats *httpapi.StatsResponse
	for _, stats := range allStats.Stats {
		if stats.Token == "WETH" {
			wethStats = &stats
			break
		}
	}

	if wethStats == nil {
		t.Fatal("WETH stats not found")
	}

	// Проверяем корректность данных WETH (2 транзакции, 4500 USD)
	if wethStats.W5m.TxCount != 2 {
		t.Errorf("Expected WETH tx count 2, got %d", wethStats.W5m.TxCount)
	}

	if wethStats.W5m.VolUSD != 4500.0 {
		t.Errorf("Expected WETH volume 4500.0, got %f", wethStats.W5m.VolUSD)
	}
}

func TestE2E_HighThroughput(t *testing.T) {
	// Тест на производительность - обрабатываем много событий за короткое время
	windows := []models.Window{models.Window5m}
	bucketStep := 1 * time.Second
	dedupTTL := time.Hour
	reorderDelay := 100 * time.Millisecond

	aggregator, err := agg.NewAggregator(windows, bucketStep, dedupTTL, reorderDelay, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	defer aggregator.Close()

	// Создаём источник событий
	sourceConfig := ingest.SourceConfig{
		EventsPerSecond: 100, // 100 событий в секунду
		TokenCount:      5,   // 5 токенов
	}
	source := ingest.NewInMemorySource(sourceConfig)

	// Запускаем генерацию событий
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	eventChan := make(chan models.Swap, 1000)

	go func() {
		source.Run(ctx, eventChan)
		close(eventChan)
	}()

	// Обрабатываем события
	eventCount := 0
	processingStart := time.Now()

	for swap := range eventChan {
		start := time.Now()
		aggregator.Ingest(context.Background(), swap)
		processingTime := time.Since(start)

		// Проверяем что обработка быстрая (< 10ms per event)
		if processingTime > 10*time.Millisecond {
			t.Errorf("Slow processing: %v for event %d", processingTime, eventCount)
		}

		eventCount++
	}

	totalTime := time.Since(processingStart)

	t.Logf("Processed %d events in %v", eventCount, totalTime)

	// Ждём завершения обработки out-of-order буфера
	time.Sleep(200 * time.Millisecond)

	// Проверяем что все события обработались
	allStats := aggregator.GetAll()
	if len(allStats) == 0 {
		t.Error("No stats found after high throughput test")
	}

	// Проверяем что статистика не пуста
	totalTx := int64(0)
	totalVol := 0.0
	realTokenCount := 0
	for token, stats := range allStats {
		// Исключаем служебные токены
		if token == "WATERMARK" {
			continue
		}
		realTokenCount++

		if stats.W5m.TxCount == 0 {
			t.Errorf("Token %s has zero transactions", token)
		}
		if stats.W5m.VolUSD == 0 {
			t.Errorf("Token %s has zero volume", token)
		}
		totalTx += stats.W5m.TxCount
		totalVol += stats.W5m.VolUSD
	}

	t.Logf("Total processed: %d transactions, $%.2f volume, %d tokens", totalTx, totalVol, realTokenCount)

	// Должно быть обработано разумное количество событий
	// Учитываем что многие события могут быть дубликатами или отброшены из-за генерации ID
	if totalTx < 10 {
		t.Errorf("Too few transactions processed: %d", totalTx)
	}

	if realTokenCount == 0 {
		t.Error("No tokens found after processing events")
	}
}

func TestE2E_HealthCheck(t *testing.T) {
	// Простой тест health check endpoint
	aggregator, err := agg.NewAggregator([]models.Window{models.Window5m}, 5*time.Second, time.Hour, 3*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create aggregator: %v", err)
	}
	defer aggregator.Close()

	router := httpapi.NewRouter(aggregator)

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	server := &http.Server{Handler: router}
	go server.Serve(listener)
	defer server.Shutdown(context.Background())

	baseURL := "http://" + listener.Addr().String()

	// Тестируем health check
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("Failed to get health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var health httpapi.HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("Failed to decode health response: %v", err)
	}

	if health.Status != "ok" {
		t.Errorf("Expected status 'ok', got '%s'", health.Status)
	}

	if health.Uptime < 0 {
		t.Errorf("Expected positive uptime, got %d", health.Uptime)
	}
}
