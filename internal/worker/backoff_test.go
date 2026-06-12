package worker

import (
	"testing"
	"time"
)

func TestBackoffExponential(t *testing.T) {
	base := 1 * time.Second
	max := 60 * time.Second
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
	}
	for _, c := range cases {
		if got := Backoff(c.attempt, base, max); got != c.want {
			t.Errorf("Backoff(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestBackoffCappedAtMax(t *testing.T) {
	if got := Backoff(100, time.Second, 30*time.Second); got != 30*time.Second {
		t.Errorf("Backoff(100) = %v, want cap of 30s", got)
	}
}

func TestBackoffNegativeAttempt(t *testing.T) {
	if got := Backoff(-5, time.Second, time.Minute); got != time.Second {
		t.Errorf("Backoff(-5) = %v, want base of 1s", got)
	}
}
