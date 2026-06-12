// Package job defines the core domain types shared by the api and the worker:
// the durable Job record and the lightweight Message that travels on Kafka.
package job

import (
	"encoding/json"
	"time"
)

// Status is the lifecycle state of a job.
//
//	queued    -> accepted, sitting on Kafka waiting for a worker
//	running   -> a worker is executing it right now
//	succeeded -> finished cleanly (terminal)
//	failed    -> a try failed; it will be retried after a backoff delay
//	dead      -> all retries exhausted; moved to the dead-letter queue (terminal)
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusDead      Status = "dead"
)

// IsTerminal reports whether a job is in a final state and must not run again.
// This is the backbone of our idempotency guard: if a duplicate Kafka message
// arrives for a terminal job, the worker skips it.
func (s Status) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusDead
}

// Job is the authoritative record of a unit of work, stored in Postgres.
type Job struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Status     Status          `json:"status"`
	Attempts   int             `json:"attempts"`
	MaxRetries int             `json:"max_retries"`
	LastError  string          `json:"last_error,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// Message is what we publish to Kafka. It is deliberately minimal: just enough
// for a worker to execute the job and update its record. The job id doubles as
// the Kafka message key so all events for one job land on the same partition
// (preserving per-job ordering).
type Message struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Attempt    int             `json:"attempt"` // 0 on first try; +1 on each retry
	MaxRetries int             `json:"max_retries"`
	EnqueuedAt time.Time       `json:"enqueued_at"`
	NotBefore  time.Time       `json:"not_before,omitempty"` // earliest processing time (backoff)
}

// Key returns the Kafka partition key for this message.
func (m Message) Key() []byte { return []byte(m.ID) }
