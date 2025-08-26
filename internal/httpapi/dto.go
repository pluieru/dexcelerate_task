package httpapi

import (
	"dexc_conf/internal/models"
	"time"
)

type StatsResponse struct {
	Token string       `json:"token"` // токен
	W5m   AggregateDTO `json:"w5m"`   // 5-минутное окно
	W1h   AggregateDTO `json:"w1h"`   // 1-часовое окно
	W24h  AggregateDTO `json:"w24h"`  // 24-часовое окно
	TS    int64        `json:"ts"`    // timestamp в миллисекундах
}

type AggregateDTO struct {
	VolUSD  float64 `json:"vol_usd"`  // объем в USD (округленный до 1e-6)
	TxCount int64   `json:"tx_count"` // количество транзакций
}

type HealthResponse struct {
	Status    string            `json:"status"`            // "ok" или "error"
	Timestamp int64             `json:"timestamp"`         // текущее время в миллисекундах
	Uptime    int64             `json:"uptime"`            // время работы в секундах
	Version   string            `json:"version,omitempty"` // версия приложения
	Details   map[string]string `json:"details,omitempty"` // дополнительная информация
}

type ErrorResponse struct {
	Error   string `json:"error"`             // сообщение об ошибке
	Code    string `json:"code,omitempty"`    // код ошибки
	Details string `json:"details,omitempty"` // дополнительные детали
}

type AllStatsResponse struct {
	Stats     []StatsResponse `json:"stats"`     // статистика по токенам
	Count     int             `json:"count"`     // количество токенов
	Timestamp int64           `json:"timestamp"` // время генерации ответа
}

func NewStatsResponse(token string, agg models.Aggregates) StatsResponse {
	return StatsResponse{
		Token: token,
		W5m:   NewAggregateDTO(agg.W5m),
		W1h:   NewAggregateDTO(agg.W1h),
		W24h:  NewAggregateDTO(agg.W24h),
		TS:    time.Now().UnixMilli(),
	}
}

func NewAggregateDTO(agg models.Aggregate) AggregateDTO {
	return AggregateDTO{
		VolUSD:  roundToMicro(agg.VolUSD),
		TxCount: agg.TxCount,
	}
}

func NewHealthResponse(startTime time.Time) HealthResponse {
	now := time.Now()
	return HealthResponse{
		Status:    "ok",
		Timestamp: now.UnixMilli(),
		Uptime:    int64(now.Sub(startTime).Seconds()),
		Version:   "1.0.0",
	}
}

func NewErrorResponse(err error, code string) ErrorResponse {
	response := ErrorResponse{
		Error: err.Error(),
	}

	if code != "" {
		response.Code = code
	}

	return response
}

func NewAllStatsResponse(allStats map[string]models.Aggregates) AllStatsResponse {
	stats := make([]StatsResponse, 0, len(allStats))

	for token, agg := range allStats {
		stats = append(stats, NewStatsResponse(token, agg))
	}

	return AllStatsResponse{
		Stats:     stats,
		Count:     len(stats),
		Timestamp: time.Now().UnixMilli(),
	}
}

func roundToMicro(value float64) float64 {
	return float64(int64(value*1e6)) / 1e6
}
