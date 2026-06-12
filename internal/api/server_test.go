package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aryan3650/conveyor/internal/job"
	"github.com/aryan3650/conveyor/internal/observability"
	"github.com/aryan3650/conveyor/internal/store"
)

// Build the Prometheus collectors exactly once for the whole test binary
// (promauto registers on the default registry, which rejects duplicates).
var testMetrics = observability.NewMetrics()

// --- in-memory fakes implementing the API's interfaces ---

type fakeStore struct {
	mu   sync.Mutex
	jobs map[string]job.Job
}

func newFakeStore() *fakeStore { return &fakeStore{jobs: make(map[string]job.Job)} }

func (f *fakeStore) Enqueue(_ context.Context, j job.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j.Status = job.StatusQueued
	f.jobs[j.ID] = j
	return nil
}

func (f *fakeStore) Get(_ context.Context, id string) (job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return job.Job{}, store.ErrNotFound
	}
	return j, nil
}

func (f *fakeStore) Counts(_ context.Context) (map[job.Status]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[job.Status]int)
	for _, j := range f.jobs {
		out[j.Status]++
	}
	return out, nil
}

func (f *fakeStore) Ping(_ context.Context) error { return nil }

type fakeProducer struct {
	mu        sync.Mutex
	published []job.Message
	err       error
}

func (f *fakeProducer) Publish(_ context.Context, _ string, m job.Message) error {
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, m)
	return nil
}

func newTestServer() (http.Handler, *fakeStore, *fakeProducer) {
	st := newFakeStore()
	p := &fakeProducer{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewServer(st, p, "jobs", testMetrics, log, 5).Routes(), st, p
}

// --- tests ---

func TestEnqueueHappyPath(t *testing.T) {
	h, st, p := newTestServer()

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs",
		bytes.NewReader([]byte(`{"type":"send_email","payload":{"to":"a@b.com"}}`)))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	var resp enqueueResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID == "" || resp.Status != job.StatusQueued {
		t.Errorf("unexpected response: %+v", resp)
	}
	if len(st.jobs) != 1 {
		t.Errorf("expected 1 persisted job, got %d", len(st.jobs))
	}
	if len(p.published) != 1 {
		t.Errorf("expected 1 published message, got %d", len(p.published))
	}
}

func TestEnqueueMissingType(t *testing.T) {
	h, _, _ := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader([]byte(`{"payload":{}}`)))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestEnqueueBadJSON(t *testing.T) {
	h, _, _ := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader([]byte(`{not json`)))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestGetJobNotFound(t *testing.T) {
	h, _, _ := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/does-not-exist", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestEnqueueThenGet(t *testing.T) {
	h, _, _ := newTestServer()

	// enqueue
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/jobs",
		bytes.NewReader([]byte(`{"type":"resize_image"}`))))
	var resp enqueueResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)

	// get
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/v1/jobs/"+resp.ID, nil))
	if w2.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200", w2.Code)
	}
	var got job.Job
	if err := json.Unmarshal(w2.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if got.ID != resp.ID || got.Type != "resize_image" {
		t.Errorf("unexpected job: %+v", got)
	}
}

func TestHealthz(t *testing.T) {
	h, _, _ := newTestServer()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}
