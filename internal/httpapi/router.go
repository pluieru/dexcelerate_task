package httpapi

import (
	"context"
	"dexc_conf/internal/agg"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Router создает и настраивает HTTP роутер
func NewRouter(aggregator agg.Aggregator) *chi.Mux {
	r := chi.NewRouter()

	handlers := NewHandlers(aggregator)

	// Middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", handlers.HealthCheck)
	r.Get("/stats", handlers.StatsHandler)

	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		handlers.writeError(w, http.StatusNotFound, "endpoint not found", "not_found")
	})

	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		handlers.writeError(w, http.StatusMethodNotAllowed, "method not allowed", "method_not_allowed")
	})

	return r
}

type Server struct {
	server *http.Server
}

func NewServer(addr string, handler http.Handler) *Server {
	return &Server{
		server: &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}
}

func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return s.server.Shutdown(ctx)
}

func (s *Server) Addr() string {
	return s.server.Addr
}
