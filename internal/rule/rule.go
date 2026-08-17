// Package rule defines the alerting rules that StreamRail evaluates over each
// closed window: a filter to select events, an aggregate to compute, and a
// HAVING condition to decide whether to alert. See docs/spec.md (rules.yaml)
// and docs/data-model.md.
package rule

import (
	"fmt"
	"strconv"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// Aggregate function names.
const (
	AggCount = "count"
	AggSum   = "sum"
)

// HAVING comparison operators.
const (
	OpGT  = "gt"
	OpGTE = "gte"
	OpLT  = "lt"
	OpLTE = "lte"
)

// Filter selects events by an exact field match. An empty Field matches all
// events (no filtering).
type Filter struct {
	Field string // "service" | "level" | any key in Event.Fields
	Eq    string
}

// Having is the post-aggregation threshold condition.
type Having struct {
	Op    string
	Value float64
}

// Rule is a single alerting rule.
type Rule struct {
	Name     string
	Filter   Filter
	GroupBy  string
	AggFunc  string // AggCount | AggSum
	AggField string // numeric field name, required for AggSum
	Having   Having
	Emit     string // "console" for Phase 3
}

// Match reports whether ev passes the rule's filter.
func (r Rule) Match(ev model.Event) bool {
	if r.Filter.Field == "" {
		return true
	}
	v, ok := fieldString(ev, r.Filter.Field)
	return ok && v == r.Filter.Eq
}

// Satisfied evaluates the HAVING condition against an aggregate value.
func (h Having) Satisfied(value float64) bool {
	switch h.Op {
	case OpGT:
		return value > h.Value
	case OpGTE:
		return value >= h.Value
	case OpLT:
		return value < h.Value
	case OpLTE:
		return value <= h.Value
	default:
		return false
	}
}

// Symbol returns the operator's mathematical symbol for alert formatting.
func (h Having) Symbol() string {
	switch h.Op {
	case OpGT:
		return ">"
	case OpGTE:
		return ">="
	case OpLT:
		return "<"
	case OpLTE:
		return "<="
	default:
		return "?"
	}
}

// fieldString returns an event field as a string. service/level are promoted
// struct fields; anything else is looked up in Event.Fields.
func fieldString(ev model.Event, field string) (string, bool) {
	switch field {
	case "service":
		return ev.Service, true
	case "level":
		return ev.Level, true
	default:
		v, ok := ev.Fields[field]
		if !ok {
			return "", false
		}
		return fmt.Sprint(v), true
	}
}

// FieldFloat returns an event field coerced to float64, for SUM aggregation.
func FieldFloat(ev model.Event, field string) (float64, bool) {
	switch field {
	case "service", "level":
		return 0, false // non-numeric struct fields
	default:
		v, ok := ev.Fields[field]
		if !ok {
			return 0, false
		}
		switch n := v.(type) {
		case float64:
			return n, true
		case int:
			return float64(n), true
		case int64:
			return float64(n), true
		case string:
			f, err := strconv.ParseFloat(n, 64)
			return f, err == nil
		default:
			return 0, false
		}
	}
}
