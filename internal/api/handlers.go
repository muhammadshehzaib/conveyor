package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/muhammadshehzaib/conveyor/internal/job"
	"github.com/muhammadshehzaib/conveyor/internal/store"
)

// enqueueRequest is the POST /v1/jobs body.
type enqueueRequest struct {
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	MaxRetries *int            `json:"max_retries"` // optional override; nil -> server default
}

type enqueueResponse struct {
	ID     string     `json:"id"`
	Status job.Status `json:"status"`
}

// handleEnqueue accepts a job, persists it as "queued", and publishes it to Kafka.
// Order matters: we write to Postgres FIRST so a status lookup right after the
// 202 always finds the job, then publish. If publishing fails we surface 503.
func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if strings.TrimSpace(req.Type) == "" {
		writeError(w, http.StatusBadRequest, "field 'type' is required")
		return
	}

	maxRetries := s.maxRetries
	if req.MaxRetries != nil && *req.MaxRetries >= 0 {
		maxRetries = *req.MaxRetries
	}

	id := uuid.NewString()
	j := job.Job{ID: id, Type: req.Type, Payload: req.Payload, MaxRetries: maxRetries}
	if err := s.store.Enqueue(r.Context(), j); err != nil {
		s.log.Error("enqueue: persist failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to persist job")
		return
	}

	msg := job.Message{
		ID:         id,
		Type:       req.Type,
		Payload:    req.Payload,
		Attempt:    0,
		MaxRetries: maxRetries,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := s.producer.Publish(r.Context(), s.topic, msg); err != nil {
		s.log.Error("enqueue: publish failed", "err", err, "job_id", id)
		writeError(w, http.StatusServiceUnavailable, "failed to enqueue job")
		return
	}

	s.metrics.Enqueued.WithLabelValues(req.Type).Inc()
	writeJSON(w, http.StatusAccepted, enqueueResponse{ID: id, Status: job.StatusQueued})
}

// handleGetJob returns the current state of a job.
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.store.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		s.log.Error("get job failed", "err", err, "job_id", id)
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// handleStats returns job counts grouped by status (handy for a quick demo and
// for sanity-checking the dashboards).
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.store.Counts(r.Context())
	if err != nil {
		s.log.Error("stats failed", "err", err)
		writeError(w, http.StatusInternalServerError, "stats failed")
		return
	}
	byStatus := map[string]int{
		"queued": 0, "running": 0, "succeeded": 0, "failed": 0, "dead": 0,
	}
	total := 0
	for st, n := range counts {
		byStatus[string(st)] = n
		total += n
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": total, "by_status": byStatus})
}

// handleHealthz is a liveness probe: the process is up and serving.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is a readiness probe: dependencies (Postgres) are reachable.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
