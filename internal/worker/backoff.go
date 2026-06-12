package worker

import (
	"math"
	"time"
)

// Backoff returns how long to wait before retrying a given attempt, using
// exponential growth: base * 2^attempt, capped at max. attempt is 0-based, so:
//
//	Backoff(0) = base          (1st retry)
//	Backoff(1) = base * 2
//	Backoff(2) = base * 4 ...
//
// Exponential backoff stops a flaky dependency (a database, an email provider)
// from being hammered while it's struggling, and gives it time to recover.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := float64(base) * math.Pow(2, float64(attempt))
	if math.IsInf(d, 1) || d > float64(max) {
		return max
	}
	return time.Duration(d)
}
