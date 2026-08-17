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
