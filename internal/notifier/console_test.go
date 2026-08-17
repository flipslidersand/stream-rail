package notifier_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/aggregator"
	"github.com/flipslidersand/stream-rail/internal/notifier"
	"github.com/flipslidersand/stream-rail/internal/rule"
	"github.com/flipslidersand/stream-rail/internal/window"
)

func result(value float64) aggregator.Result {
	start := time.Date(2024, 6, 10, 10, 0, 0, 0, time.UTC)
	return aggregator.Result{
		Key:       window.Key{GroupKey: "api", WindowStart: start},
		WindowEnd: start.Add(5 * time.Minute),
		Count:     int64(value),
		Value:     value,
	}
}

var errorSpike = rule.Rule{
	Name:    "error-spike",
	GroupBy: "service",
	AggFunc: rule.AggCount,
	Having:  rule.Having{Op: rule.OpGT, Value: 20},
}

func TestEmit_FiresAndFormats(t *testing.T) {
	var buf bytes.Buffer
	c := notifier.NewConsole(&buf)

	if fired := c.Emit(errorSpike, result(21)); !fired {
		t.Fatal("expected alert to fire for 21 > 20")
	}
	got := strings.TrimSpace(buf.String())
	want := "[ALERT] rule=error-spike service=api count=21 > 20 (10:00-10:05)"
	if got != want {
		t.Fatalf("alert line mismatch:\n got: %q\nwant: %q", got, want)
	}
}

func TestEmit_BelowThresholdSilent(t *testing.T) {
	var buf bytes.Buffer
	c := notifier.NewConsole(&buf)

	if fired := c.Emit(errorSpike, result(20)); fired { // 20 > 20 is false
		t.Fatal("did not expect alert for 20 > 20")
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no output, got %q", buf.String())
	}
}
