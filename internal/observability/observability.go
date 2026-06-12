// Package observability centralizes structured logging and Prometheus metrics so
// the api and worker report themselves the same way. "If you can't measure it,
// you can't run it in production" — this package is what makes the dashboards work.
package observability

import (
	"log/slog"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// NewLogger returns a JSON structured logger at the given level (debug|info|warn|error).
// JSON logs are the norm in production because log aggregators (Loki, ELK, Datadog)
// parse them automatically.
func NewLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// Metrics holds every Prometheus collector the system reports. Construct it once
// per process with NewMetrics.
type Metrics struct {
	Enqueued     *prometheus.CounterVec   // labels: type
	Processed    *prometheus.CounterVec   // labels: type, result (succeeded|failed|dead)
	Duration     *prometheus.HistogramVec // labels: type
	Retried      prometheus.Counter
	DeadLettered prometheus.Counter
	InFlight     prometheus.Gauge
}

// NewMetrics registers all collectors on the default Prometheus registry, which
// is what /metrics exposes.
func NewMetrics() *Metrics {
	return &Metrics{
		Enqueued: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "conveyor_jobs_enqueued_total",
			Help: "Total number of jobs enqueued.",
		}, []string{"type"}),

		Processed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "conveyor_jobs_processed_total",
			Help: "Total jobs processed, labelled by terminal result.",
		}, []string{"type", "result"}),

		Duration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "conveyor_job_duration_seconds",
			Help:    "Job handler execution time in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"type"}),

		Retried: promauto.NewCounter(prometheus.CounterOpts{
			Name: "conveyor_jobs_retried_total",
			Help: "Total retries scheduled (job failed but had attempts remaining).",
		}),

		DeadLettered: promauto.NewCounter(prometheus.CounterOpts{
			Name: "conveyor_jobs_dead_total",
			Help: "Total jobs sent to the dead-letter queue.",
		}),

		InFlight: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "conveyor_jobs_inflight",
			Help: "Number of jobs currently being processed.",
		}),
	}
}
