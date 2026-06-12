// Package store is the Postgres persistence layer. It owns the authoritative job
// records and, crucially, the atomic "claim" that makes processing idempotent.
package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aryan3650/conveyor/internal/job"
	"github.com/aryan3650/conveyor/migrations"
)

// ErrNotFound is returned by Get when no job with the given id exists.
var ErrNotFound = errors.New("job not found")

// Store wraps a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to Postgres and verifies the connection.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// Ping checks connectivity (used by health checks).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate applies any embedded .sql migrations that have not yet run. Each file
// runs once, inside a transaction, and is recorded in schema_migrations.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		return err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files) // lexical order == migration order (0001_, 0002_, ...)

	for _, name := range files {
		var applied bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, name).
			Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}

		sqlBytes, err := migrations.FS.ReadFile(name)
		if err != nil {
			return err
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Enqueue inserts a brand-new job in the "queued" state. It is a no-op if the id
// already exists (ON CONFLICT DO NOTHING), so re-submitting the same id is safe.
func (s *Store) Enqueue(ctx context.Context, j job.Job) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status, attempts, max_retries, created_at, updated_at)
		VALUES ($1, $2, $3::jsonb, $4, 0, $5, now(), now())
		ON CONFLICT (id) DO NOTHING`,
		j.ID, j.Type, payloadOrEmpty(j.Payload), job.StatusQueued, j.MaxRetries)
	return err
}

// Claim is the idempotency guard. It atomically moves a job to "running" ONLY if
// it is not already in a terminal state. The single SQL statement:
//   - inserts the row if it somehow doesn't exist yet, OR
//   - flips an existing non-terminal row to "running".
//
// If the job already succeeded or is dead, zero rows are affected and we report
// claimed=false, so the worker skips this duplicate Kafka delivery. This is how
// we get safe at-least-once processing.
func (s *Store) Claim(ctx context.Context, m job.Message) (bool, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (id, type, payload, status, attempts, max_retries, created_at, updated_at)
		VALUES ($1, $2, $3::jsonb, 'running', $4, $5, now(), now())
		ON CONFLICT (id) DO UPDATE
			SET status = 'running', attempts = $4, updated_at = now()
			WHERE jobs.status NOT IN ('succeeded', 'dead')
		RETURNING id`,
		m.ID, m.Type, payloadOrEmpty(m.Payload), m.Attempt+1, m.MaxRetries).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // already terminal -> duplicate delivery, skip
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// MarkSucceeded records a terminal success.
func (s *Store) MarkSucceeded(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status='succeeded', last_error='', updated_at=now() WHERE id=$1`, id)
	return err
}

// MarkFailed records a failed attempt that will be retried.
func (s *Store) MarkFailed(ctx context.Context, id string, attempts int, errMsg string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status='failed', attempts=$2, last_error=$3, updated_at=now() WHERE id=$1`,
		id, attempts, errMsg)
	return err
}

// MarkDead records a terminal failure (retries exhausted -> dead-letter queue).
func (s *Store) MarkDead(ctx context.Context, id string, attempts int, errMsg string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status='dead', attempts=$2, last_error=$3, updated_at=now() WHERE id=$1`,
		id, attempts, errMsg)
	return err
}

// Get returns a single job by id.
func (s *Store) Get(ctx context.Context, id string) (job.Job, error) {
	var j job.Job
	err := s.pool.QueryRow(ctx, `
		SELECT id, type, payload, status, attempts, max_retries, last_error, created_at, updated_at
		FROM jobs WHERE id=$1`, id).
		Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxRetries,
			&j.LastError, &j.CreatedAt, &j.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, ErrNotFound
	}
	return j, err
}

// Counts returns the number of jobs in each status (powers /stats and dashboards).
func (s *Store) Counts(ctx context.Context) (map[job.Status]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[job.Status]int)
	for rows.Next() {
		var st job.Status
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

// payloadOrEmpty guarantees we always store valid JSON (jsonb rejects empty input).
func payloadOrEmpty(p []byte) string {
	if len(p) == 0 {
		return "{}"
	}
	return string(p)
}
