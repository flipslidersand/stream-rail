// Package aggregator reduces a closed window Batch to a single numeric value
// (COUNT, SUM, MAX, MIN, or AVG) after applying the rule's filter. See
// docs/implementation-guide.md Phase 3 and docs/data-model.md (AggResult).
package aggregator

import (
	"math"
	"time"

	"github.com/flipslidersand/stream-rail/internal/rule"
	"github.com/flipslidersand/stream-rail/internal/window"
)

// Result is the aggregate of one window under one rule.
type Result struct {
	Key       window.Key
	WindowEnd time.Time
	Count     int64   // number of events that passed the filter
	Value     float64 // the aggregated value checked by HAVING
	Corrected bool    // re-emission triggered by a late event
}

// Aggregate applies r's filter to the batch events and computes the configured
// aggregation function (COUNT, SUM, MAX, MIN, or AVG).
func Aggregate(b window.Batch, r rule.Rule) Result {
	var count int64
	var sum float64
	max := math.Inf(-1)
	min := math.Inf(1)

	for _, ev := range b.Events {
		if !r.Match(ev) {
			continue
		}
		count++
		switch r.AggFunc {
		case rule.AggSum, rule.AggAvg:
			if f, ok := rule.FieldFloat(ev, r.AggField); ok {
				sum += f
			}
		case rule.AggMax:
			if f, ok := rule.FieldFloat(ev, r.AggField); ok && f > max {
				max = f
			}
		case rule.AggMin:
			if f, ok := rule.FieldFloat(ev, r.AggField); ok && f < min {
				min = f
			}
		}
	}

	res := Result{Key: b.Key, WindowEnd: b.WindowEnd, Count: count, Corrected: b.Corrected}
	switch r.AggFunc {
	case rule.AggSum:
		res.Value = sum
	case rule.AggMax:
		if count > 0 {
			res.Value = max
		}
	case rule.AggMin:
		if count > 0 {
			res.Value = min
		}
	case rule.AggAvg:
		if count > 0 {
			res.Value = sum / float64(count)
		}
	default: // AggCount
		res.Value = float64(count)
	}
	return res
}
