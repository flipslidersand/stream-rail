// Package notifier evaluates a rule's HAVING condition against an aggregate
// result and emits an alert. Phase 3 supports console output. See
// docs/implementation-guide.md Phase 3.
package notifier

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/flipslidersand/stream-rail/internal/aggregator"
	"github.com/flipslidersand/stream-rail/internal/rule"
)

// Console prints alerts to an io.Writer (stdout by default).
type Console struct {
	out io.Writer
}

// NewConsole builds a Console. A nil writer defaults to os.Stdout.
func NewConsole(out io.Writer) *Console {
	if out == nil {
		out = os.Stdout
	}
	return &Console{out: out}
}

// Emit checks the HAVING condition and, if satisfied, prints an alert.
// It reports whether an alert fired.
//
// Example: [ALERT] rule=error-spike service=api count=21 > 20 (10:00-10:05)
func (c *Console) Emit(r rule.Rule, res aggregator.Result) bool {
	if !r.Having.Satisfied(res.Value) {
		return false
	}

	groupBy := r.GroupBy
	if groupBy == "" {
		groupBy = "group"
	}
	suffix := ""
	if res.Corrected {
		suffix = " (corrected)"
	}
	fmt.Fprintf(c.out, "[ALERT] rule=%s %s=%s %s=%s %s %s (%s-%s)%s\n",
		r.Name,
		groupBy, res.Key.GroupKey,
		r.AggFunc, formatNum(res.Value),
		r.Having.Symbol(), formatNum(r.Having.Value),
		res.Key.WindowStart.Format("15:04"),
		res.WindowEnd.Format("15:04"),
		suffix,
	)
	return true
}

// formatNum renders whole numbers without a trailing ".0".
func formatNum(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
