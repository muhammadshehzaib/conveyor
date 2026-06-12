// Command worker consumes jobs from Kafka and executes them, applying retry and
// dead-letter policy. Run as many copies as you like — they form a consumer group
// and Kafka splits the partitions among them. That's the horizontal-scaling story.
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/aryan3650/conveyor/internal/config"
	"github.com/aryan3650/conveyor/internal/observability"
	"github.com/aryan3650/conveyor/internal/queue"
	"github.com/aryan3650/conveyor/internal/store"
	"github.com/aryan3650/conveyor/internal/worker"
)

func main() {
	cfg := config.Load()
	log := observability.NewLogger(cfg.LogLevel)

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

	if err := queue.EnsureTopics(ctx, cfg.KafkaBrokers, 6, 1, cfg.JobsTopic, cfg.RetryTopic, cfg.DLQTopic); err != nil {
		log.Error("ensure topics", "err", err)
		os.Exit(1)
	}

	producer := queue.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()
	metrics := observability.NewMetrics()

	registry := worker.NewRegistry()
	worker.RegisterDemoHandlers(registry)

	// The job consumers share cfg.KafkaGroup. The retry forwarder uses its own
	// group so it doesn't compete for the jobs topic's partitions.
	jobsReader := queue.NewReader(cfg.KafkaBrokers, cfg.KafkaGroup, cfg.JobsTopic)
	retryReader := queue.NewReader(cfg.KafkaBrokers, cfg.KafkaGroup+".retry", cfg.RetryTopic)

	engine := worker.NewEngine(worker.Config{
		Concurrency: cfg.WorkerConcurrency,
		JobTimeout:  30 * time.Second,
		BackoffBase: 1 * time.Second,
		BackoffMax:  60 * time.Second,
		JobsTopic:   cfg.JobsTopic,
		RetryTopic:  cfg.RetryTopic,
		DLQTopic:    cfg.DLQTopic,
	}, jobsReader, retryReader, producer, st, registry, metrics, log)

	// Expose /metrics and /healthz so Prometheus can scrape the worker.
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	metricsSrv := &http.Server{Addr: cfg.WorkerMetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("worker metrics listening", "addr", cfg.WorkerMetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", "err", err)
		}
	}()

	log.Info("worker started",
		"concurrency", cfg.WorkerConcurrency,
		"group", cfg.KafkaGroup,
		"topic", cfg.JobsTopic,
	)

	// engine.Run blocks until ctx is cancelled and in-flight jobs have drained.
	done := make(chan struct{})
	go func() {
		_ = engine.Run(ctx)
		close(done)
	}()

	<-ctx.Done()
	log.Info("shutdown signal received, draining worker")
	<-done // wait for the engine to finish draining

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	log.Info("worker stopped")
}
