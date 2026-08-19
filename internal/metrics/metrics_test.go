package metrics_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	smetrics "github.com/flipslidersand/stream-rail/internal/metrics"
)

func TestMetrics_Counters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := smetrics.New(reg)

	m.EventsTotal.WithLabelValues("http").Add(3)
	m.EventsTotal.WithLabelValues("nats").Add(5)
	m.DedupedTotal.Add(2)
	m.WindowsClosedTotal.WithLabelValues("false").Add(4)
	m.WindowsClosedTotal.WithLabelValues("true").Add(1)
	m.LateEventsTotal.Add(1)

	if got := testutil.ToFloat64(m.EventsTotal.WithLabelValues("http")); got != 3 {
		t.Errorf("events http = %g, want 3", got)
	}
	if got := testutil.ToFloat64(m.EventsTotal.WithLabelValues("nats")); got != 5 {
		t.Errorf("events nats = %g, want 5", got)
	}
	if got := testutil.ToFloat64(m.DedupedTotal); got != 2 {
		t.Errorf("deduped = %g, want 2", got)
	}
	if got := testutil.ToFloat64(m.WindowsClosedTotal.WithLabelValues("false")); got != 4 {
		t.Errorf("windows_closed false = %g, want 4", got)
	}
	if got := testutil.ToFloat64(m.LateEventsTotal); got != 1 {
		t.Errorf("late_events = %g, want 1", got)
	}
}

func TestMetrics_NamesRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := smetrics.New(reg)

	// Trigger at least one observation so each family appears in gather output.
	m.EventsTotal.WithLabelValues("http").Inc()
	m.DedupedTotal.Inc()
	m.WindowsClosedTotal.WithLabelValues("false").Inc()
	m.LateEventsTotal.Inc()

	if err := testutil.CollectAndCompare(m.DedupedTotal,
		strings.NewReader(`# HELP streamrail_deduped_total Total number of NATS messages skipped by deduplication.
# TYPE streamrail_deduped_total counter
streamrail_deduped_total 1
`)); err != nil {
		t.Errorf("deduped counter mismatch: %v", err)
	}
}
