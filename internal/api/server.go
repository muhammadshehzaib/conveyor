// Package api implements the HTTP front door: enqueue jobs, query their status,
// and expose health + metrics endpoints.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/muhammadshehzaib/conveyor/internal/job"
	"github.com/muhammadshehzaib/conveyor/internal/observability"
)

// JobStore is the slice of persistence the API needs. Depending on an interface
// (not *store.Store) keeps the HTTP layer testable with an in-memory fake.
type JobStore interface {
	Enqueue(ctx context.Context, j job.Job) error
	Get(ctx context.Context, id string) (job.Job, error)
	Counts(ctx context.Context) (map[job.Status]int, error)
	Ping(ctx context.Context) error
}

// Publisher is the slice of Kafka the API needs.
type Publisher interface {
	Publish(ctx context.Context, topic string, m job.Message) error
}

// Server holds the dependencies every handler needs.
type Server struct {
	store      JobStore
	producer   Publisher
	topic      string
	metrics    *observability.Metrics
	log        *slog.Logger
	maxRetries int
}

// NewServer wires up the API server. The concrete *store.Store and
// *queue.Producer satisfy the JobStore and Publisher interfaces.
func NewServer(st JobStore, p Publisher, topic string, m *observability.Metrics, log *slog.Logger, maxRetries int) *Server {
	return &Server{store: st, producer: p, topic: topic, metrics: m, log: log, maxRetries: maxRetries}
}

// Routes builds the HTTP handler. Go 1.22+ pattern routing lets us match method +
// path (and capture {id}) with the standard library — no third-party router needed.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/jobs", s.handleEnqueue)
	mux.HandleFunc("GET /v1/jobs/{id}", s.handleGetJob)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promhttp.Handler())
	return s.recoverer(s.requestLogger(mux))
}

// --- middleware ---

// statusRecorder captures the response status code for access logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// requestLogger logs one structured line per request with method, path, status,
// and latency.
func (s *Server) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("http_request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// recoverer turns a handler panic into a 500 instead of crashing the server.
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic recovered", "err", rec, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
