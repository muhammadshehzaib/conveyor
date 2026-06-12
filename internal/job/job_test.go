package job

import "testing"

func TestStatusIsTerminal(t *testing.T) {
	terminal := []Status{StatusSucceeded, StatusDead}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("%q should be terminal", s)
		}
	}
	nonTerminal := []Status{StatusQueued, StatusRunning, StatusFailed}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("%q should not be terminal", s)
		}
	}
}

func TestMessageKeyIsJobID(t *testing.T) {
	m := Message{ID: "job-123"}
	if got := string(m.Key()); got != "job-123" {
		t.Errorf("Key() = %q, want %q", got, "job-123")
	}
}
