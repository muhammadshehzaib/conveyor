package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/muhammadshehzaib/conveyor/internal/job"
)

// testStore connects to a Postgres instance for integration tests. It uses
// CONVEYOR_TEST_DB_DSN (set in CI) or the local docker-compose database, and
// skips the test entirely if no database is reachable — so `go test ./...` never
// fails just because Postgres isn't running.
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("CONVEYOR_TEST_DB_DSN")
	if dsn == "" {
		dsn = "postgres://conveyor:conveyor@localhost:5432/conveyor?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	st, err := Open(ctx, dsn)
	if err != nil {
		t.Skipf("postgres not available, skipping integration test: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func TestEnqueueAndGet(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	id := uuid.NewString()

	if err := st.Enqueue(ctx, job.Job{ID: id, Type: "send_email", MaxRetries: 5}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != job.StatusQueued {
		t.Errorf("status = %q, want queued", got.Status)
	}
	if got.Type != "send_email" {
		t.Errorf("type = %q, want send_email", got.Type)
	}
}

func TestGetNotFound(t *testing.T) {
	st := testStore(t)
	if _, err := st.Get(context.Background(), uuid.NewString()); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// TestClaimIdempotency is the heart of correctness: once a job is terminal, a
// duplicate Kafka delivery must NOT be re-claimed. This is what guarantees a job
// never runs twice under at-least-once delivery.
func TestClaimIdempotency(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	id := uuid.NewString()
	msg := job.Message{ID: id, Type: "send_email", MaxRetries: 5}

	if err := st.Enqueue(ctx, job.Job{ID: id, Type: "send_email", MaxRetries: 5}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// First delivery: claim succeeds.
	claimed, err := st.Claim(ctx, msg)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !claimed {
		t.Fatal("first claim should succeed")
	}

	// Job completes.
	if err := st.MarkSucceeded(ctx, id); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}

	// Duplicate delivery of the SAME job: claim must be refused.
	claimed, err = st.Claim(ctx, msg)
	if err != nil {
		t.Fatalf("duplicate claim: %v", err)
	}
	if claimed {
		t.Fatal("duplicate claim must be refused once the job is terminal")
	}
}

func TestRetryThenSucceedTransitions(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	id := uuid.NewString()
	msg := job.Message{ID: id, Type: "charge_payment", MaxRetries: 5}
	_ = st.Enqueue(ctx, job.Job{ID: id, Type: "charge_payment", MaxRetries: 5})

	// Attempt 1: claim, fail.
	if ok, _ := st.Claim(ctx, msg); !ok {
		t.Fatal("claim 1 should succeed")
	}
	if err := st.MarkFailed(ctx, id, 1, "boom"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	j, _ := st.Get(ctx, id)
	if j.Status != job.StatusFailed || j.LastError != "boom" {
		t.Errorf("after failure: status=%q lastErr=%q", j.Status, j.LastError)
	}

	// Attempt 2: a failed job is non-terminal, so it can be re-claimed.
	msg.Attempt = 1
	if ok, _ := st.Claim(ctx, msg); !ok {
		t.Fatal("claim 2 should succeed (failed is not terminal)")
	}
	if err := st.MarkSucceeded(ctx, id); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}
	j, _ = st.Get(ctx, id)
	if j.Status != job.StatusSucceeded {
		t.Errorf("final status = %q, want succeeded", j.Status)
	}
}
