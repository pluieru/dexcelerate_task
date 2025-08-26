package store

import (
	"context"
	"dexc_conf/internal/common"
	"dexc_conf/internal/models"
	"sync"
	"time"
)

// InMemoryStateStore реализует StateStore в памяти
type InMemoryStateStore struct {
	mu   sync.RWMutex
	data map[string]map[models.Window]map[time.Time]Bucket // token -> window -> timestamp -> bucket
}

// NewInMemoryStateStore создает новое in-memory хранилище состояния
func NewInMemoryStateStore() *InMemoryStateStore {
	return &InMemoryStateStore{
		data: make(map[string]map[models.Window]map[time.Time]Bucket),
	}
}

// Add добавляет данные в бакет для токена и окна
func (s *InMemoryStateStore) Add(ctx context.Context, token string, window models.Window, bucket Bucket) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Инициализируем структуры если нужно
	if _, exists := s.data[token]; !exists {
		s.data[token] = make(map[models.Window]map[time.Time]Bucket)
	}

	if _, exists := s.data[token][window]; !exists {
		s.data[token][window] = make(map[time.Time]Bucket)
	}

	// Получаем существующий бакет или создаем новый
	existing, exists := s.data[token][window][bucket.Start]
	if exists {
		// Добавляем к существующему
		existing.Add(bucket.VolUSD, bucket.TxCount)
		s.data[token][window][bucket.Start] = existing
	} else {
		// Создаем новый
		s.data[token][window][bucket.Start] = bucket
	}

	return nil
}

// LoadRange загружает бакеты для токена и окна в указанном временном диапазоне
func (s *InMemoryStateStore) LoadRange(ctx context.Context, token string, window models.Window, from, to time.Time) ([]Bucket, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var buckets []Bucket

	// Проверяем существование токена и окна
	tokenData, tokenExists := s.data[token]
	if !tokenExists {
		return buckets, nil
	}

	windowData, windowExists := tokenData[window]
	if !windowExists {
		return buckets, nil
	}

	// Собираем бакеты в указанном диапазоне
	for timestamp, bucket := range windowData {
		if (timestamp.Equal(from) || timestamp.After(from)) &&
			(timestamp.Equal(to) || timestamp.Before(to)) {
			buckets = append(buckets, bucket)
		}
	}

	return buckets, nil
}

// GetTokens возвращает список всех токенов в хранилище
func (s *InMemoryStateStore) GetTokens(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tokens := make([]string, 0, len(s.data))
	for token := range s.data {
		tokens = append(tokens, token)
	}

	return tokens, nil
}

// Snapshot сохраняет снапшот состояния
func (s *InMemoryStateStore) Snapshot(ctx context.Context, snap Snapshot) error {
	// Для in-memory store мы делегируем сохранение снапшота SnapshotManager
	// Этот метод используется для совместимости с интерфейсом
	return common.ErrSnapshotNotFound
}

// LoadSnapshot загружает снапшот состояния
func (s *InMemoryStateStore) LoadSnapshot(ctx context.Context) (Snapshot, error) {
	// Для in-memory store мы делегируем загрузку снапшота SnapshotManager
	// Этот метод используется для совместимости с интерфейсом
	return Snapshot{}, common.ErrSnapshotNotFound
}

// LoadFromSnapshot загружает данные из снапшота
func (s *InMemoryStateStore) LoadFromSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Очищаем существующие данные
	s.data = make(map[string]map[models.Window]map[time.Time]Bucket)

	// Загружаем данные из снапшота
	for token, tokenData := range snap.Data {
		s.data[token] = make(map[models.Window]map[time.Time]Bucket)

		for window, buckets := range tokenData {
			s.data[token][window] = make(map[time.Time]Bucket)

			for _, bucket := range buckets {
				s.data[token][window][bucket.Start] = bucket
			}
		}
	}

	return nil
}

// CreateSnapshot создает снапшот текущего состояния
func (s *InMemoryStateStore) CreateSnapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := Snapshot{
		Data: make(map[string]map[models.Window][]Bucket),
	}

	// Копируем все данные в снапшот
	for token, tokenData := range s.data {
		snapshot.Data[token] = make(map[models.Window][]Bucket)

		for window, windowData := range tokenData {
			buckets := make([]Bucket, 0, len(windowData))
			for _, bucket := range windowData {
				buckets = append(buckets, bucket)
			}
			snapshot.Data[token][window] = buckets
		}
	}

	return snapshot
}

// Clear очищает все данные
func (s *InMemoryStateStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]map[models.Window]map[time.Time]Bucket)
}

// GetStats возвращает статистику хранилища
func (s *InMemoryStateStore) GetStats() StateStoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := StateStoreStats{
		TokenCount:  len(s.data),
		WindowStats: make(map[models.Window]WindowStats),
	}

	// Собираем статистику по окнам
	windowCounts := make(map[models.Window]int)
	windowBuckets := make(map[models.Window]int)

	for _, tokenData := range s.data {
		for window, windowData := range tokenData {
			windowCounts[window]++
			windowBuckets[window] += len(windowData)
		}
	}

	for window, tokenCount := range windowCounts {
		stats.WindowStats[window] = WindowStats{
			Window:      window,
			TokenCount:  tokenCount,
			BucketCount: windowBuckets[window],
		}
		stats.TotalBuckets += windowBuckets[window]
	}

	return stats
}

// Close закрывает хранилище
func (s *InMemoryStateStore) Close() error {
	s.Clear()
	return nil
}

// InMemoryOffsetStore реализует OffsetStore в памяти
type InMemoryOffsetStore struct {
	mu        sync.RWMutex
	watermark time.Time
}

// NewInMemoryOffsetStore создает новое in-memory хранилище offset
func NewInMemoryOffsetStore() *InMemoryOffsetStore {
	return &InMemoryOffsetStore{}
}

// SaveWatermark сохраняет последнюю обработанную временную метку
func (s *InMemoryOffsetStore) SaveWatermark(ctx context.Context, ts time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watermark = ts
	return nil
}

// LoadWatermark загружает последнюю обработанную временную метку
func (s *InMemoryOffsetStore) LoadWatermark(ctx context.Context) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.watermark, nil
}

// Close закрывает хранилище
func (s *InMemoryOffsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watermark = time.Time{}
	return nil
}

// StateStoreStats содержит статистику хранилища состояния
type StateStoreStats struct {
	TokenCount   int                           `json:"token_count"`   // количество токенов
	TotalBuckets int                           `json:"total_buckets"` // общее количество бакетов
	WindowStats  map[models.Window]WindowStats `json:"window_stats"`  // статистика по окнам
}

// WindowStats содержит статистику окна
type WindowStats struct {
	Window      models.Window `json:"window"`       // окно
	TokenCount  int           `json:"token_count"`  // количество токенов в окне
	BucketCount int           `json:"bucket_count"` // количество бакетов в окне
}
