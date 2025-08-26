package ingest

import (
	"context"
	"dexc_conf/internal/models"
)

type EventSource interface {
	Run(ctx context.Context, out chan<- models.Swap) error
}

type BackpressureStrategy string

const (
	BackpressureBlock  BackpressureStrategy = "block"  // блокироваться при полном канале
	BackpressureDrop   BackpressureStrategy = "drop"   // дропать события (текущее поведение)
	BackpressureBuffer BackpressureStrategy = "buffer" // использовать внутренний буфер
)

type SourceConfig struct {
	EventsPerSecond        int                  `json:"events_per_second"`        // количество событий в секунду для демо
	TokenCount             int                  `json:"token_count"`              // количество уникальных токенов 
	SeedData               string               `json:"seed_data"`                // путь к файлу с начальными данными (опционально)
	BackpressureStrategy   BackpressureStrategy `json:"backpressure_strategy"`    // стратегия обработки backpressure
	BackpressureBufferSize int                  `json:"backpressure_buffer_size"` // размер внутреннего буфера для стратегии buffer
}
