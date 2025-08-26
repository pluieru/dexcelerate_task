package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"dexc_conf/internal/agg"
	"dexc_conf/internal/common"
	"dexc_conf/internal/httpapi"
	"dexc_conf/internal/ingest"
	"dexc_conf/internal/models"
	"dexc_conf/internal/store"
	"dexc_conf/internal/ws"
)

type App struct {
	config      *common.Config
	logger      *slog.Logger
	snapshotMgr *store.SnapshotManager
	aggregator  agg.Aggregator
	eventSource ingest.EventSource
	wsHub       *ws.Hub
	wsPublisher ws.Publisher
	httpServer  *httpapi.Server

	ctx    context.Context
	cancel context.CancelFunc
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := common.LoadConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := config.Validate(); err != nil {
		fmt.Printf("Invalid config: %v\n", err)
		os.Exit(1)
	}

	logger := common.SetupLogger(config.LogLevel)

	app := &App{
		config: config,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}

	if err := app.Initialize(); err != nil {
		logger.Error("Failed to initialize app", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err := app.Start(); err != nil {
		logger.Error("Failed to start app", slog.String("error", err.Error()))
		os.Exit(1)
	}

	app.WaitForShutdown()

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := app.Shutdown(shutdownCtx); err != nil {
		logger.Error("Error during shutdown", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("Application shutdown completed")
}

func (a *App) Initialize() error {
	a.logger.Info("Initializing application",
		slog.String("version", "1.0.0"),
		slog.String("go_version", runtime.Version()))

	a.snapshotMgr = store.NewSnapshotManager(a.config.SnapshotPath)

	aggregator, err := agg.NewAggregator(
		a.config.Windows,
		a.config.BucketStep,
		a.config.DedupTTL,
		a.config.ReorderDelay,
		a.config.MaxTimestampSkew,
	)
	if err != nil {
		return fmt.Errorf("failed to create aggregator: %w", err)
	}
	a.aggregator = aggregator

	if a.snapshotMgr.Exists() {
		snapshot, err := a.snapshotMgr.Load(a.ctx)
		if err != nil {
			a.logger.Warn("Failed to load snapshot", slog.String("error", err.Error()))
		} else {
			if err := a.aggregator.Restore(snapshot); err != nil {
				a.logger.Warn("Failed to restore aggregator state", slog.String("error", err.Error()))
			} else {
				a.logger.Info("Restored from snapshot",
					slog.Time("watermark", snapshot.Watermark),
					slog.Int("tokens", len(snapshot.Data)))
			}
		}
	}

	a.wsHub = ws.NewHub(a.config.WSWriteBuffer)
	a.wsPublisher = ws.NewHubPublisher(a.wsHub)

	sourceConfig := ingest.SourceConfig{
		EventsPerSecond:        a.config.EventsPerSecond,
		TokenCount:             a.config.TokenCount,
		BackpressureStrategy:   ingest.BackpressureStrategy(a.config.BackpressureStrategy),
		BackpressureBufferSize: a.config.BackpressureBufferSize,
	}
	a.eventSource = ingest.NewInMemorySource(sourceConfig)

	router := httpapi.NewRouter(a.aggregator)

	router.Get("/ws", a.wsHub.ServeWS)

	addr := ":" + a.config.Port
	a.httpServer = httpapi.NewServer(addr, router)

	a.logger.Info("Application initialized successfully",
		slog.String("port", a.config.Port),
		slog.Any("windows", a.config.Windows),
		slog.String("bucket_step", a.config.BucketStep.String()),
		slog.Int("events_per_second", a.config.EventsPerSecond))

	return nil
}

func (a *App) Start() error {
	a.logger.Info("Starting application components")

	a.wsHub.Start()

	go a.snapshotRoutine()

	swapChannel := make(chan models.Swap, 1000) // типизированный канал

	go func() {
		if err := a.eventSource.Run(a.ctx, swapChannel); err != nil && err != context.Canceled {
			a.logger.Error("Event source error", slog.String("error", err.Error()))
		}
	}()

	go a.eventProcessor(swapChannel)

	go a.statsPublisher()

	go func() {
		a.logger.Info("Starting HTTP server", slog.String("addr", a.httpServer.Addr()))
		if err := a.httpServer.Start(); err != nil && err != http.ErrServerClosed {
			a.logger.Error("HTTP server error", slog.String("error", err.Error()))
		}
	}()

	a.logger.Info("All components started successfully")
	return nil
}

func (a *App) snapshotRoutine() {
	ticker := time.NewTicker(a.config.SnapshotPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// create snapshot
			snapshot := a.aggregator.Snapshot()

			// save snapshot
			if err := a.snapshotMgr.Save(a.ctx, snapshot); err != nil {
				a.logger.Error("Failed to save snapshot", slog.String("error", err.Error()))
			} else {
				a.logger.Debug("Snapshot saved", slog.Int("tokens", len(snapshot.Data)))
			}

		case <-a.ctx.Done():
			return
		}
	}
}

// process events
func (a *App) eventProcessor(events <-chan models.Swap) {
	for {
		select {
		case swap := <-events:
			// process event through aggregator
			a.aggregator.Ingest(a.ctx, swap)

		case <-a.ctx.Done():
			return
		}
	}
}

// publish stats to WebSocket
func (a *App) statsPublisher() {
	ticker := time.NewTicker(time.Second) // publish every second
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// get all stats
			allStats := a.aggregator.GetAll()

			// publish for each token
			for token, aggregates := range allStats {
				if err := a.wsPublisher.PublishAggregate(a.ctx, token, aggregates); err != nil {
					a.logger.Debug("Failed to publish aggregate",
						slog.String("token", token),
						slog.String("error", err.Error()))
				}
			}

		case <-a.ctx.Done():
			return
		}
	}
}

// wait for shutdown
func (a *App) WaitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	a.logger.Info("Received shutdown signal", slog.String("signal", sig.String()))
	a.cancel()
}

// shutdown
func (a *App) Shutdown(ctx context.Context) error {
	a.logger.Info("Starting graceful shutdown")

	// stop HTTP server
	if a.httpServer != nil {
		a.logger.Info("Shutting down HTTP server")
		if err := a.httpServer.Shutdown(10 * time.Second); err != nil {
			a.logger.Error("HTTP server shutdown error", slog.String("error", err.Error()))
		}
	}

	// stop WebSocket hub
	if a.wsHub != nil {
		a.logger.Info("Shutting down WebSocket hub")
		a.wsHub.Stop()
	}

	// save final snapshot
	if a.aggregator != nil && a.snapshotMgr != nil {
		a.logger.Info("Saving final snapshot")
		snapshot := a.aggregator.Snapshot()
		if err := a.snapshotMgr.Save(ctx, snapshot); err != nil {
			a.logger.Error("Failed to save final snapshot", slog.String("error", err.Error()))
		}
	}

	// close aggregator
	if a.aggregator != nil {
		if err := a.aggregator.Close(); err != nil {
			a.logger.Error("Aggregator close error", slog.String("error", err.Error()))
		}
	}

	a.logger.Info("Graceful shutdown completed")
	return nil
}
