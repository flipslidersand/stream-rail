// Package aggregator reduces a closed window Batch to a single numeric value
// (COUNT or SUM) after applying the rule's filter. See docs/implementation-guide.md
// Phase 3 and docs/data-model.md (AggResult).
package aggregator

import (
	"time"

	"github.com/flipslidersand/stream-rail/internal/rule"
	"github.com/flipslidersand/stream-rail/internal/window"
)

// Result is the aggregate of one window under one rule.
type Result struct {
	Key       window.Key
	WindowEnd time.Time
	Count     int64   // number of events that passed the filter
	Value     float64 // the aggregated value (count or sum) checked by HAVING
}

// Aggregate applies r's filter to the batch events and computes COUNT or SUM.
func Aggregate(b window.Batch, r rule.Rule) Result {
	var count int64
	var sum float64
	for _, ev := range b.Events {
		if !r.Match(ev) {
			continue
		}
		count++
		if r.AggFunc == rule.AggSum {
			if f, ok := rule.FieldFloat(ev, r.AggField); ok {
				sum += f
			}
		}
	}

	res := Result{Key: b.Key, WindowEnd: b.WindowEnd, Count: count}
	if r.AggFunc == rule.AggSum {
		res.Value = sum
	} else {
		res.Value = float64(count)
	}
	return res
}
