package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/muhammadshehzaib/conveyor/internal/job"
	"github.com/muhammadshehzaib/conveyor/internal/observability"
	"github.com/muhammadshehzaib/conveyor/internal/queue"
	"github.com/muhammadshehzaib/conveyor/internal/store"
)

// Config tunes the engine's behavior.
type Config struct {
	Concurrency int           // max jobs processed in parallel per worker process
	JobTimeout  time.Duration // hard limit for a single handler run
	BackoffBase time.Duration // first retry delay
	BackoffMax  time.Duration // cap on retry delay
	JobsTopic   string
	RetryTopic  string
	DLQTopic    string
}

// Engine ties Kafka, the store, and the handler registry together. It runs two
// loops: one that processes jobs, and one that re-queues delayed retries.
type Engine struct {
	cfg         Config
	reader      *kafka.Reader // main jobs topic
	retryReader *kafka.Reader // delayed-retry topic
	producer    *queue.Producer
	store       *store.Store
	registry    *Registry
	metrics     *observability.Metrics
	log         *slog.Logger
}

// NewEngine constructs an Engine.
func NewEngine(cfg Config, reader, retryReader *kafka.Reader, p *queue.Producer, st *store.Store, reg *Registry, m *observability.Metrics, log *slog.Logger) *Engine {
	return &Engine{
		cfg: cfg, reader: reader, retryReader: retryReader,
		producer: p, store: st, registry: reg, metrics: m, log: log,
	}
}

// Run starts both loops and blocks until ctx is cancelled and all in-flight work
// has drained. This two-goroutine structure is what gives us graceful shutdown.
func (e *Engine) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); e.consumeJobs(ctx) }()
	go func() { defer wg.Done(); e.forwardRetries(ctx) }()
	wg.Wait()
	return nil
}

// consumeJobs is the hot path: fetch a message, hand it to a bounded pool of
// goroutines, and repeat. On shutdown it stops fetching and waits for in-flight
// jobs to finish before returning (no dropped work).
func (e *Engine) consumeJobs(ctx context.Context) {
	defer e.reader.Close()

	sem := make(chan struct{}, e.cfg.Concurrency) // limits parallelism
	var inflight sync.WaitGroup

	for {
		m, err := e.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // shutdown requested
			}
			e.log.Error("fetch job message", "err", err)
			continue
		}

		sem <- struct{}{} // acquire a slot (blocks when Concurrency jobs are running)
		inflight.Add(1)
		go func(km kafka.Message) {
			defer inflight.Done()
			defer func() { <-sem }()
			e.handleMessage(km)
		}(m)
	}

	inflight.Wait() // drain: let running jobs finish and commit
	e.log.Info("job consumer drained")
}

// handleMessage is the core decision tree for one job delivery.
func (e *Engine) handleMessage(km kafka.Message) {
	var msg job.Message
	if err := json.Unmarshal(km.Value, &msg); err != nil {
		// Poison message: we can't even decode it. Park the raw bytes in the DLQ
		// and commit so it never blocks the partition again.
		e.log.Error("undecodable message; dead-lettering raw bytes", "err", err, "offset", km.Offset)
		dlqCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		perr := e.producer.PublishRaw(dlqCtx, e.cfg.DLQTopic, km.Key, km.Value)
		cancel()
		if perr != nil {
			e.log.Error("publish raw to DLQ failed; leaving uncommitted", "err", perr)
			return
		}
		e.metrics.DeadLettered.Inc()
		e.commit(km)
		return
	}

	// 1) Idempotency claim. If the job already reached a terminal state, this is a
	//    duplicate delivery — skip it and commit.
	claimed, err := e.claim(msg)
	if err != nil {
		// Transient store error: do NOT commit, so Kafka redelivers later.
		e.log.Error("claim failed; leaving uncommitted for redelivery", "err", err, "job_id", msg.ID)
		return
	}
	if !claimed {
		e.log.Debug("duplicate/terminal job skipped", "job_id", msg.ID)
		e.commit(km)
		return
	}

	// 2) Find the handler. Unknown type -> straight to the DLQ.
	handler, ok := e.registry.Get(msg.Type)
	if !ok {
		e.deadLetter(msg, "no handler registered for type: "+msg.Type)
		e.commit(km)
		return
	}

	// 3) Execute with a hard timeout. A separate background context (not the
	//    shutdown context) lets an in-flight job finish during a graceful drain.
	e.metrics.InFlight.Inc()
	start := time.Now()
	runCtx, cancel := context.WithTimeout(context.Background(), e.cfg.JobTimeout)
	runErr := safeRun(runCtx, handler, msg)
	cancel()
	e.metrics.InFlight.Dec()
	e.metrics.Duration.WithLabelValues(msg.Type).Observe(time.Since(start).Seconds())

	// 4) Success.
	if runErr == nil {
		e.markSucceeded(msg.ID)
		e.metrics.Processed.WithLabelValues(msg.Type, "succeeded").Inc()
		e.commit(km)
		return
	}

	// 5) Failure with retries left -> schedule a delayed retry.
	if msg.Attempt < msg.MaxRetries {
		if err := e.scheduleRetry(msg, runErr.Error()); err != nil {
			e.log.Error("schedule retry failed; leaving uncommitted", "err", err, "job_id", msg.ID)
			return // redelivered
		}
		e.commit(km)
		return
	}

	// 6) Retries exhausted -> dead-letter.
	e.deadLetter(msg, runErr.Error())
	e.commit(km)
}

// forwardRetries reads the retry topic, waits until each message's NotBefore
// (the backoff delay) has elapsed, then re-publishes it to the main jobs topic.
// This is how we implement delayed redelivery on top of Kafka, which has no
// native per-message delay.
func (e *Engine) forwardRetries(ctx context.Context) {
	defer e.retryReader.Close()

	for {
		m, err := e.retryReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			e.log.Error("fetch retry message", "err", err)
			continue
		}

		var msg job.Message
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			e.log.Error("undecodable retry message; dropping", "err", err)
			e.commitTo(e.retryReader, m)
			continue
		}

		// Sleep until the job is due (interruptible by shutdown).
		if wait := time.Until(msg.NotBefore); wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return // leave uncommitted; it'll be reprocessed after restart
			}
		}

		forwarded := msg
		forwarded.NotBefore = time.Time{} // due now
		pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = e.producer.Publish(pubCtx, e.cfg.JobsTopic, forwarded)
		cancel()
		if err != nil {
			e.log.Error("forward retry to jobs topic failed; leaving uncommitted", "err", err, "job_id", msg.ID)
			continue
		}
		e.commitTo(e.retryReader, m)
	}
	e.log.Info("retry forwarder drained")
}

// --- small helpers, each owning a bounded context ---

func (e *Engine) claim(msg job.Message) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return e.store.Claim(ctx, msg)
}

func (e *Engine) markSucceeded(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.store.MarkSucceeded(ctx, id); err != nil {
		e.log.Error("mark succeeded failed", "err", err, "job_id", id)
	}
}

// scheduleRetry publishes the next attempt to the retry topic with a backoff
// delay and records the failure in Postgres.
func (e *Engine) scheduleRetry(msg job.Message, cause string) error {
	delay := Backoff(msg.Attempt, e.cfg.BackoffBase, e.cfg.BackoffMax)
	next := msg
	next.Attempt = msg.Attempt + 1
	next.NotBefore = time.Now().UTC().Add(delay)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.producer.Publish(ctx, e.cfg.RetryTopic, next); err != nil {
		return err
	}
	if err := e.store.MarkFailed(ctx, msg.ID, msg.Attempt+1, cause); err != nil {
		e.log.Error("mark failed failed", "err", err, "job_id", msg.ID)
	}
	e.metrics.Retried.Inc()
	e.metrics.Processed.WithLabelValues(msg.Type, "failed").Inc()
	e.log.Warn("job failed; retry scheduled",
		"job_id", msg.ID, "next_attempt", next.Attempt, "delay", delay.String(), "cause", cause)
	return nil
}

// deadLetter parks a permanently failed job in the DLQ and records it as dead.
func (e *Engine) deadLetter(msg job.Message, cause string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.producer.Publish(ctx, e.cfg.DLQTopic, msg); err != nil {
		e.log.Error("publish to DLQ failed", "err", err, "job_id", msg.ID)
	}
	if err := e.store.MarkDead(ctx, msg.ID, msg.Attempt+1, cause); err != nil {
		e.log.Error("mark dead failed", "err", err, "job_id", msg.ID)
	}
	e.metrics.DeadLettered.Inc()
	e.metrics.Processed.WithLabelValues(msg.Type, "dead").Inc()
	e.log.Error("job dead-lettered", "job_id", msg.ID, "attempts", msg.Attempt+1, "cause", cause)
}

func (e *Engine) commit(km kafka.Message) { e.commitTo(e.reader, km) }

func (e *Engine) commitTo(r *kafka.Reader, km kafka.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.CommitMessages(ctx, km); err != nil {
		e.log.Error("commit offset failed", "err", err, "topic", km.Topic, "offset", km.Offset)
	}
}

// safeRun executes a handler, converting a panic into an error so one bad job
// can't take down the worker.
func safeRun(ctx context.Context, h Handler, msg job.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return h(ctx, msg)
}
