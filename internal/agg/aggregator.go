package agg

import (
	"context"
	"dexc_conf/internal/models"
	"dexc_conf/internal/store"
	"log/slog"
	"sync"
	"time"
)

// Aggregator processes swap events and calculates statistics
type Aggregator interface {
	// Ingest processes a swap event
	Ingest(ctx context.Context, swap models.Swap)

	// Get returns current aggregates for a token
	Get(token string) models.Aggregates

	// GetAll returns aggregates for all tokens
	GetAll() map[string]models.Aggregates

	// Snapshot creates a snapshot of current state
	Snapshot() store.Snapshot

	// Restore restores state from snapshot
	Restore(snap store.Snapshot) error

	// Close shuts down the aggregator
	Close() error
}

// TokenAggregator holds aggregators for a single token
type TokenAggregator struct {
	Token   string
	Rings   map[models.Window]*RingBuffer
	Current models.Aggregates
}

// aggregator implements the Aggregator interface
type aggregator struct {
	mu sync.RWMutex

	windows          []models.Window
	bucketStep       time.Duration
	maxTimestampSkew time.Duration

	dedup   *Deduplicator
	reorder *ReorderBuffer

	tokens map[string]*TokenAggregator

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewAggregator creates a new aggregator
func NewAggregator(windows []models.Window, bucketStep time.Duration, dedupTTL, reorderDelay, maxTimestampSkew time.Duration) (Aggregator, error) {
	agg := &aggregator{
		windows:          windows,
		bucketStep:       bucketStep,
		maxTimestampSkew: maxTimestampSkew,
		dedup:            NewDeduplicator(dedupTTL),
		reorder:          NewReorderBuffer(reorderDelay),
		tokens:           make(map[string]*TokenAggregator),
		stopCh:           make(chan struct{}),
	}

	agg.startBackgroundTasks()

	return agg, nil
}

// Ingest processes a swap event
func (a *aggregator) Ingest(ctx context.Context, swap models.Swap) {
	if !swap.IsValid() {
		slog.Debug("Invalid swap event", slog.String("swap_id", swap.ID))
		return
	}

	now := time.Now().UTC()
	if swap.TS.After(now.Add(a.maxTimestampSkew)) {
		slog.Warn("Future event detected, adjusting timestamp",
			slog.String("swap_id", swap.ID),
			slog.Time("original_ts", swap.TS),
			slog.Time("adjusted_ts", now.Add(a.maxTimestampSkew)))

		swap.TS = now.Add(a.maxTimestampSkew)
	}

	key := swap.Key()
	if a.dedup.IsDuplicate(key) {
		return
	}
	ready, dropped := a.reorder.Add(swap)

	for _, droppedSwap := range dropped {
		slog.Debug("Dropped out-of-order event",
			slog.String("key", droppedSwap.Key()),
			slog.Time("event_time", droppedSwap.TS),
			slog.Time("watermark", a.reorder.GetWatermark()))
	}
	for _, readySwap := range ready {
		a.processSwap(readySwap)
	}
}

// processSwap обрабатывает событие свопа (внутренний метод)
func (a *aggregator) processSwap(swap models.Swap) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Получаем или создаем агрегатор для токена
	tokenAgg, exists := a.tokens[swap.Token]
	if !exists {
		tokenAgg = a.createTokenAggregator(swap.Token)
		a.tokens[swap.Token] = tokenAgg
	}

	// Выравниваем время к началу бакета
	bucketStart := models.AlignToBucket(swap.TS, a.bucketStep)

	// Обновляем все окна для токена
	for window, ring := range tokenAgg.Rings {
		// Добавляем в кольцевой буфер
		evicted := ring.Add(bucketStart, swap.USD, 1)

		// Вычитаем вытесненные бакеты из текущего агрегата
		for _, bucket := range evicted {
			tokenAgg.Current.GetByWindow(window)
			agg := tokenAgg.Current.GetByWindow(window)
			agg.Subtract(bucket.VolUSD, bucket.TxCount)
			tokenAgg.Current.SetByWindow(window, agg)
		}

		// Добавляем новые значения к текущему агрегату
		agg := tokenAgg.Current.GetByWindow(window)
		agg.Add(swap.USD, 1)
		tokenAgg.Current.SetByWindow(window, agg)
	}

	slog.Debug("Processed swap",
		slog.String("token", swap.Token),
		slog.Float64("usd", swap.USD),
		slog.Time("bucket_start", bucketStart))
}

// createTokenAggregator создает новый агрегатор для токена
func (a *aggregator) createTokenAggregator(token string) *TokenAggregator {
	tokenAgg := &TokenAggregator{
		Token: token,
		Rings: make(map[models.Window]*RingBuffer),
	}

	// Создаем кольцевые буферы для каждого окна
	for _, window := range a.windows {
		ring, err := NewRingBuffer(window, a.bucketStep)
		if err != nil {
			slog.Error("Failed to create ring buffer",
				slog.String("token", token),
				slog.String("window", string(window)),
				slog.String("error", err.Error()))
			continue
		}
		tokenAgg.Rings[window] = ring
	}

	return tokenAgg
}

// Get возвращает текущие агрегаты для токена
func (a *aggregator) Get(token string) models.Aggregates {
	a.mu.RLock()
	defer a.mu.RUnlock()

	tokenAgg, exists := a.tokens[token]
	if !exists {
		return models.Aggregates{}
	}

	return tokenAgg.Current
}

// GetAll возвращает агрегаты для всех токенов
func (a *aggregator) GetAll() map[string]models.Aggregates {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make(map[string]models.Aggregates)
	for token, tokenAgg := range a.tokens {
		result[token] = tokenAgg.Current
	}

	return result
}

// Snapshot создает снапшот текущего состояния
func (a *aggregator) Snapshot() store.Snapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()

	snapshot := store.Snapshot{
		Windows:    a.windows,
		BucketStep: a.bucketStep,
		Watermark:  a.reorder.GetWatermark(),
		Data:       make(map[string]map[models.Window][]store.Bucket),
	}

	// Сохраняем данные всех токенов
	for token, tokenAgg := range a.tokens {
		tokenData := make(map[models.Window][]store.Bucket)
		for window, ring := range tokenAgg.Rings {
			tokenData[window] = ring.GetBuckets()
		}
		snapshot.Data[token] = tokenData
	}

	return snapshot
}

// Restore восстанавливает состояние из снапшота
func (a *aggregator) Restore(snap store.Snapshot) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Очищаем текущее состояние
	a.tokens = make(map[string]*TokenAggregator)

	// Восстанавливаем watermark
	a.reorder.SetWatermark(snap.Watermark)

	// Восстанавливаем данные токенов
	for token, tokenData := range snap.Data {
		tokenAgg := a.createTokenAggregator(token)

		// Загружаем бакеты в кольцевые буферы
		for window, buckets := range tokenData {
			if ring, exists := tokenAgg.Rings[window]; exists {
				ring.LoadBuckets(buckets)

				// Пересчитываем агрегат
				agg := ring.GetAggregate()
				tokenAgg.Current.SetByWindow(window, agg)
			}
		}

		a.tokens[token] = tokenAgg
	}

	slog.Info("Restored aggregator state",
		slog.Int("tokens", len(a.tokens)),
		slog.Time("watermark", snap.Watermark))

	return nil
}

// startBackgroundTasks запускает фоновые задачи
func (a *aggregator) startBackgroundTasks() {
	// Очистка дедупликатора
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.dedup.StartCleanupRoutine(time.Minute, a.stopCh)
	}()

	// Периодический flush reorder буфера
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ready := a.reorder.PeriodicFlush(time.Second)
				for _, swap := range ready {
					a.processSwap(swap)
				}
			case <-a.stopCh:
				return
			}
		}
	}()
}

// Close закрывает агрегатор
func (a *aggregator) Close() error {
	close(a.stopCh)
	a.wg.Wait()

	// Обрабатываем оставшиеся события в reorder буфере
	remaining := a.reorder.Flush()
	for _, swap := range remaining {
		a.processSwap(swap)
	}

	return nil
}
