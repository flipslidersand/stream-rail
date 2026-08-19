package rule_test

import (
	"testing"

	"github.com/flipslidersand/stream-rail/internal/model"
	"github.com/flipslidersand/stream-rail/internal/rule"
)

func TestMatch(t *testing.T) {
	r := rule.Rule{Filter: rule.Filter{Field: "level", Eq: "ERROR"}}
	if !r.Match(model.Event{Level: "ERROR"}) {
		t.Error("ERROR event should match")
	}
	if r.Match(model.Event{Level: "INFO"}) {
		t.Error("INFO event should not match")
	}
	// Empty filter matches everything.
	if !(rule.Rule{}).Match(model.Event{Level: "INFO"}) {
		t.Error("empty filter should match all")
	}
}

func TestHaving_Satisfied(t *testing.T) {
	cases := []struct {
		op    string
		thr   float64
		value float64
		want  bool
	}{
		{rule.OpGT, 20, 21, true},
		{rule.OpGT, 20, 20, false},
		{rule.OpGTE, 20, 20, true},
		{rule.OpLT, 5, 4, true},
		{rule.OpLTE, 5, 5, true},
		{rule.OpLTE, 5, 6, false},
	}
	for _, c := range cases {
		h := rule.Having{Op: c.op, Value: c.thr}
		if got := h.Satisfied(c.value); got != c.want {
			t.Errorf("%s(%g,%g) = %v, want %v", c.op, c.value, c.thr, got, c.want)
		}
	}
}

func TestGroupKey(t *testing.T) {
	ev := model.Event{Service: "api", Level: "ERROR", Fields: map[string]any{"region": "us", "host": "h1"}}

	if got := rule.GroupKey(ev, nil); got != "api" {
		t.Errorf("nil fields = %q, want api (service default)", got)
	}
	if got := rule.GroupKey(ev, []string{"service"}); got != "api" {
		t.Errorf("single = %q, want api", got)
	}
	if got := rule.GroupKey(ev, []string{"service", "region"}); got != "api|us" {
		t.Errorf("service+region = %q, want api|us", got)
	}
	// Absent field contributes empty string.
	if got := rule.GroupKey(ev, []string{"service", "missing"}); got != "api|" {
		t.Errorf("missing field = %q, want api|", got)
	}
}

func TestFieldFloat(t *testing.T) {
	ev := model.Event{Fields: map[string]any{"n": float64(1.5), "i": 3, "s": "2.5", "bad": "x"}}
	for _, c := range []struct {
		field string
		want  float64
		ok    bool
	}{
		{"n", 1.5, true},
		{"i", 3, true},
		{"s", 2.5, true},
		{"bad", 0, false},
		{"missing", 0, false},
		{"level", 0, false},
	} {
		got, ok := rule.FieldFloat(ev, c.field)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("FieldFloat(%q) = %g,%v want %g,%v", c.field, got, ok, c.want, c.ok)
		}
	}
}
