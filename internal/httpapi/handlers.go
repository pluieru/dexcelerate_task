package httpapi

import (
	"dexc_conf/internal/agg"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type Handlers struct {
	aggregator agg.Aggregator
	startTime  time.Time
}

func NewHandlers(aggregator agg.Aggregator) *Handlers {
	return &Handlers{
		aggregator: aggregator,
		startTime:  time.Now(),
	}
}

func (h *Handlers) HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := NewHealthResponse(h.startTime)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode health response", slog.String("error", err.Error()))
	}
}

func (h *Handlers) GetStats(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" {
		h.writeError(w, http.StatusBadRequest, "token parameter is required", "missing_token")
		return
	}

	aggregates := h.aggregator.Get(token)

	response := NewStatsResponse(token, aggregates)
		
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode stats response",
			slog.String("token", token),
			slog.String("error", err.Error()))
	}
}

func (h *Handlers) GetAllStats(w http.ResponseWriter, r *http.Request) {
	allStats := h.aggregator.GetAll()

	response := NewAllStatsResponse(allStats)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode all stats response", slog.String("error", err.Error()))
	}
}

func (h *Handlers) StatsHandler(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" {
		h.GetAllStats(w, r)
	} else {
		h.GetStats(w, r)
	}
}

func (h *Handlers) writeError(w http.ResponseWriter, statusCode int, message, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := ErrorResponse{
		Error: message,
		Code:  code,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode error response",
			slog.String("error", err.Error()))
	}
}
