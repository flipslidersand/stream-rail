// Package notifier defines the Notifier interface and the data passed to alert
// sinks. Concrete implementations are Console and Webhook.
package notifier

import (
	"time"

	"github.com/flipslidersand/stream-rail/internal/aggregator"
	"github.com/flipslidersand/stream-rail/internal/rule"
)

// Notifier emits an alert when a HAVING condition is satisfied.
// It reports whether an alert fired.
type Notifier interface {
	Emit(r rule.Rule, res aggregator.Result) bool
}

// AlertData is the template context passed to webhook payloads.
type AlertData struct {
	RuleName    string
	GroupKey    string
	AggFunc     string
	Value       float64
	Threshold   float64
	Op          string
	WindowStart time.Time
	WindowEnd   time.Time
	Corrected   bool
}

func newAlertData(r rule.Rule, res aggregator.Result) AlertData {
	return AlertData{
		RuleName:    r.Name,
		GroupKey:    res.Key.GroupKey,
		AggFunc:     r.AggFunc,
		Value:       res.Value,
		Threshold:   r.Having.Value,
		Op:          r.Having.Symbol(),
		WindowStart: res.Key.WindowStart,
		WindowEnd:   res.WindowEnd,
		Corrected:   res.Corrected,
	}
}
