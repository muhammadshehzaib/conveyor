package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aryan3650/conveyor/internal/job"
)

// demoPayload lets a client steer the demo handler's behavior from the job
// payload, so you can deterministically show off success, retries, and the DLQ:
//
//	{"sleep_ms": 50}                  -> succeeds after simulating 50ms of work
//	{"fail_times": 2}                 -> fails twice, then succeeds (retry demo)
//	{"fail_always": true}             -> always fails -> ends in the DLQ
type demoPayload struct {
	SleepMS    int  `json:"sleep_ms"`
	FailTimes  int  `json:"fail_times"`
	FailAlways bool `json:"fail_always"`
}

// demoHandler simulates real work. Swap this out for your own handlers (sending
// email, resizing images, charging cards) — the engine doesn't care what the
// handler does, only whether it returns an error.
func demoHandler(ctx context.Context, m job.Message) error {
	var p demoPayload
	_ = json.Unmarshal(m.Payload, &p) // empty/invalid payload -> zero value -> just succeed

	if p.SleepMS > 0 {
		select {
		case <-time.After(time.Duration(p.SleepMS) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err() // respect the job timeout / shutdown
		}
	}

	if p.FailAlways {
		return fmt.Errorf("simulated permanent failure")
	}
	// m.Attempt is 0-based, so fail_times=2 fails on attempts 0 and 1.
	if m.Attempt < p.FailTimes {
		return fmt.Errorf("simulated transient failure (attempt %d of %d)", m.Attempt, p.FailTimes)
	}
	return nil
}

// RegisterDemoHandlers wires the demo handler to a few representative job types.
// Any type NOT registered here will be dead-lettered, which itself demonstrates
// the "unknown handler" safety path.
func RegisterDemoHandlers(r *Registry) {
	for _, t := range []string{"send_email", "resize_image", "charge_payment", "generate_report"} {
		r.Register(t, demoHandler)
	}
}
