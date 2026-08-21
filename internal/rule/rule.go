// Package rule defines the alerting rules that StreamRail evaluates over each
// closed window: a filter to select events, an aggregate to compute, and a
// HAVING condition to decide whether to alert. See docs/spec.md (rules.yaml)
// and docs/data-model.md.
package rule

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// Aggregate function names.
const (
	AggCount = "count"
	AggSum   = "sum"
	AggMax   = "max"
	AggMin   = "min"
	AggAvg   = "avg"
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
	Name       string
	Filter     Filter
	GroupBy    []string // one or more fields; empty defaults to ["service"]
	AggFunc    string   // AggCount | AggSum
	AggField   string   // numeric field name, required for AggSum
	Having     Having
	Emit       string        // "console" for Phase 3
	WindowSize time.Duration // tumbling window size; 0 = engine default
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

// GroupValue returns the grouping key for an event under the given field.
// An empty field groups by service (the historical default). A field that is
// absent from the event yields "" so such events share one "unknown" bucket
// rather than being dropped.
func GroupValue(ev model.Event, field string) string {
	if field == "" {
		field = "service"
	}
	v, _ := fieldString(ev, field)
	return v
}

// GroupKey returns a composite grouping key for an event by joining the values
// of all fields with "|". Absent fields contribute "" so events still bucket
// predictably. An empty or nil fields slice falls back to the "service" field.
func GroupKey(ev model.Event, fields []string) string {
	if len(fields) == 0 {
		return GroupValue(ev, "service")
	}
	if len(fields) == 1 {
		return GroupValue(ev, fields[0])
	}
	parts := make([]string, len(fields))
	for i, f := range fields {
		parts[i] = GroupValue(ev, f)
	}
	return strings.Join(parts, "|")
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
