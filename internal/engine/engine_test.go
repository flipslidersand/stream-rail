package engine

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
	"github.com/flipslidersand/stream-rail/internal/rule"
)

func TestNormalizeGroupBy(t *testing.T) {
	if got := normalizeGroupBy(""); got != "service" {
		t.Errorf("normalizeGroupBy(\"\") = %q, want service", got)
	}
	if got := normalizeGroupBy("host"); got != "host" {
		t.Errorf("normalizeGroupBy(host) = %q, want host", got)
	}
}

func TestStreamKeyNamespace(t *testing.T) {
	a := streamKey{size: 5 * time.Minute, groupBy: "service"}
	b := streamKey{size: 5 * time.Minute, groupBy: "host"}
	if a.namespace() == b.namespace() {
		t.Fatalf("namespaces must differ by group_by: %q == %q", a.namespace(), b.namespace())
	}
	if !strings.Contains(b.namespace(), "host") {
		t.Errorf("namespace %q should embed group_by", b.namespace())
	}
}

func TestGroupFuncFor(t *testing.T) {
	ev := model.Event{Service: "api", Level: "ERROR", Fields: map[string]any{"host": "h1"}}
	if got := groupFuncFor("service")(ev); got != "api" {
		t.Errorf("group by service = %q, want api", got)
	}
	if got := groupFuncFor("level")(ev); got != "ERROR" {
		t.Errorf("group by level = %q, want ERROR", got)
	}
	if got := groupFuncFor("host")(ev); got != "h1" {
		t.Errorf("group by host = %q, want h1", got)
	}
	if got := groupFuncFor("")(ev); got != "api" {
		t.Errorf("empty group_by should default to service, got %q", got)
	}
}

// safeBuffer is a concurrency-safe io.Writer for capturing console output the
// engine writes from its own goroutine.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestEngine_GroupsByArbitraryField verifies that a rule grouping by a Fields
// key (host) aggregates per host rather than per service (#17).
func TestEngine_GroupsByArbitraryField(t *testing.T) {
	in := make(chan model.Event, 16)
	out := &safeBuffer{}

	rules := []rule.Rule{{
		Name:       "host-spike",
		GroupBy:    "host",
		AggFunc:    rule.AggCount,
		Having:     rule.Having{Op: rule.OpGT, Value: 1},
		Emit:       "console",
		WindowSize: 50 * time.Millisecond,
	}}
	eng := New(in, 50*time.Millisecond, rules, out, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = eng.Run(ctx) }()

	ts := time.Now().Add(-time.Second).Unix()
	ev := func(host string) model.Event {
		return model.Event{Service: "api", Level: "ERROR", Timestamp: ts, Fields: map[string]any{"host": host}}
	}
	// h1 gets 2 events (> 1 → alert); h2 gets 1 (no alert).
	in <- ev("h1")
	in <- ev("h1")
	in <- ev("h2")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(out.String(), "host=h1") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	got := out.String()
	if !strings.Contains(got, "host=h1") {
		t.Fatalf("expected an alert grouped by host=h1, got:\n%s", got)
	}
	if strings.Contains(got, "host=h2") {
		t.Errorf("host=h2 (count=1) should not alert, got:\n%s", got)
	}
}
