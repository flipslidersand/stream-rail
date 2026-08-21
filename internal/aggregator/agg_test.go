package aggregator_test

import (
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/aggregator"
	"github.com/flipslidersand/stream-rail/internal/model"
	"github.com/flipslidersand/stream-rail/internal/rule"
	"github.com/flipslidersand/stream-rail/internal/window"
)

func batch(events ...model.Event) window.Batch {
	start := time.Date(2024, 6, 10, 10, 0, 0, 0, time.UTC)
	return window.Batch{
		Key:       window.Key{GroupKey: "api", WindowStart: start},
		WindowEnd: start.Add(5 * time.Minute),
		Events:    events,
		Count:     int64(len(events)),
	}
}

func TestAggregate_CountAppliesFilter(t *testing.T) {
	b := batch(
		model.Event{Service: "api", Level: "ERROR"},
		model.Event{Service: "api", Level: "INFO"}, // filtered out
		model.Event{Service: "api", Level: "ERROR"},
	)
	r := rule.Rule{
		Filter:  rule.Filter{Field: "level", Eq: "ERROR"},
		AggFunc: rule.AggCount,
	}
	res := aggregator.Aggregate(b, r)
	if res.Count != 2 || res.Value != 2 {
		t.Fatalf("count=%d value=%g, want 2/2", res.Count, res.Value)
	}
}

func TestAggregate_SumOverField(t *testing.T) {
	b := batch(
		model.Event{Service: "api", Level: "ERROR", Fields: map[string]any{"bytes": float64(100)}},
		model.Event{Service: "api", Level: "ERROR", Fields: map[string]any{"bytes": 50}},
		model.Event{Service: "api", Level: "INFO", Fields: map[string]any{"bytes": float64(999)}}, // filtered
	)
	r := rule.Rule{
		Filter:   rule.Filter{Field: "level", Eq: "ERROR"},
		AggFunc:  rule.AggSum,
		AggField: "bytes",
	}
	res := aggregator.Aggregate(b, r)
	if res.Value != 150 || res.Count != 2 {
		t.Fatalf("sum=%g count=%d, want 150/2", res.Value, res.Count)
	}
}

func TestAggregate_NoFilterCountsAll(t *testing.T) {
	b := batch(
		model.Event{Service: "api", Level: "ERROR"},
		model.Event{Service: "api", Level: "INFO"},
	)
	res := aggregator.Aggregate(b, rule.Rule{AggFunc: rule.AggCount})
	if res.Value != 2 {
		t.Fatalf("value=%g, want 2", res.Value)
	}
}

func TestAggregate_Max(t *testing.T) {
	b := batch(
		model.Event{Service: "api", Level: "ERROR", Fields: map[string]any{"latency_ms": float64(300)}},
		model.Event{Service: "api", Level: "ERROR", Fields: map[string]any{"latency_ms": float64(800)}},
		model.Event{Service: "api", Level: "INFO", Fields: map[string]any{"latency_ms": float64(999)}}, // filtered
	)
	r := rule.Rule{
		Filter:   rule.Filter{Field: "level", Eq: "ERROR"},
		AggFunc:  rule.AggMax,
		AggField: "latency_ms",
	}
	res := aggregator.Aggregate(b, r)
	if res.Value != 800 || res.Count != 2 {
		t.Fatalf("max=%g count=%d, want 800/2", res.Value, res.Count)
	}
}

func TestAggregate_Min(t *testing.T) {
	b := batch(
		model.Event{Service: "api", Level: "ERROR", Fields: map[string]any{"latency_ms": float64(300)}},
		model.Event{Service: "api", Level: "ERROR", Fields: map[string]any{"latency_ms": float64(800)}},
		model.Event{Service: "api", Level: "INFO", Fields: map[string]any{"latency_ms": float64(1)}}, // filtered
	)
	r := rule.Rule{
		Filter:   rule.Filter{Field: "level", Eq: "ERROR"},
		AggFunc:  rule.AggMin,
		AggField: "latency_ms",
	}
	res := aggregator.Aggregate(b, r)
	if res.Value != 300 || res.Count != 2 {
		t.Fatalf("min=%g count=%d, want 300/2", res.Value, res.Count)
	}
}

func TestAggregate_Avg(t *testing.T) {
	b := batch(
		model.Event{Service: "api", Level: "ERROR", Fields: map[string]any{"latency_ms": float64(200)}},
		model.Event{Service: "api", Level: "ERROR", Fields: map[string]any{"latency_ms": float64(400)}},
		model.Event{Service: "api", Level: "INFO", Fields: map[string]any{"latency_ms": float64(999)}}, // filtered
	)
	r := rule.Rule{
		Filter:   rule.Filter{Field: "level", Eq: "ERROR"},
		AggFunc:  rule.AggAvg,
		AggField: "latency_ms",
	}
	res := aggregator.Aggregate(b, r)
	if res.Value != 300 || res.Count != 2 {
		t.Fatalf("avg=%g count=%d, want 300/2", res.Value, res.Count)
	}
}

func TestAggregate_MaxNoMatchingEvents(t *testing.T) {
	b := batch(model.Event{Service: "api", Level: "INFO"})
	r := rule.Rule{
		Filter:   rule.Filter{Field: "level", Eq: "ERROR"},
		AggFunc:  rule.AggMax,
		AggField: "latency_ms",
	}
	res := aggregator.Aggregate(b, r)
	if res.Count != 0 || res.Value != 0 {
		t.Fatalf("expected zero result for no matching events, got count=%d value=%g", res.Count, res.Value)
	}
}
