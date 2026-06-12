// Package worker contains the job-processing engine: consume from Kafka, run the
// right handler, and apply retry / dead-letter policy.
package worker

import (
	"context"

	"github.com/aryan3650/conveyor/internal/job"
)

// Handler executes a single job. Returning a non-nil error triggers the retry /
// dead-letter machinery. Because delivery is at-least-once, a handler may run for
// the same job more than once — write handlers to be idempotent where it matters.
type Handler func(ctx context.Context, m job.Message) error

// Registry maps a job type (e.g. "send_email") to the Handler that runs it.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register binds a handler to a job type. Last registration wins.
func (r *Registry) Register(jobType string, h Handler) {
	r.handlers[jobType] = h
}

// Get looks up the handler for a job type.
func (r *Registry) Get(jobType string) (Handler, bool) {
	h, ok := r.handlers[jobType]
	return h, ok
}
