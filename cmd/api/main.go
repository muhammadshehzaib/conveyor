// Command api is the HTTP front door for Conveyor. It persists incoming jobs and
// publishes them to Kafka for workers to process.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/muhammadshehzaib/conveyor/internal/api"
	"github.com/muhammadshehzaib/conveyor/internal/config"
	"github.com/muhammadshehzaib/conveyor/internal/observability"
	"github.com/muhammadshehzaib/conveyor/internal/queue"
	"github.com/muhammadshehzaib/conveyor/internal/store"
)

func main() {
	cfg := config.Load()
	log := observability.NewLogger(cfg.LogLevel)

	// signal.NotifyContext cancels ctx on Ctrl-C / SIGTERM, which drives a clean
	// shutdown of the HTTP server below.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DBDSN)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		log.Error("run migrations", "err", err)
		os.Exit(1)
	}

	// Create topics up front so the very first publish succeeds.
	if err := queue.EnsureTopics(ctx, cfg.KafkaBrokers, 6, 1, cfg.JobsTopic, cfg.RetryTopic, cfg.DLQTopic); err != nil {
		log.Error("ensure topics", "err", err)
		os.Exit(1)
	}

	producer := queue.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	metrics := observability.NewMetrics()
	srv := api.NewServer(st, producer, cfg.JobsTopic, metrics, log, cfg.MaxRetries)

	httpServer := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("api listening", "addr", cfg.APIAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done() // block until a shutdown signal arrives
	log.Info("shutdown signal received, draining api")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
	log.Info("api stopped")
}
