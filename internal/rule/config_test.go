package rule_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/rule"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rules.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadFile_Valid(t *testing.T) {
	path := writeConfig(t, `
rules:
  - name: error-spike
    source: application_logs
    window: { type: tumbling, size: 5m }
    filter: { field: level, eq: ERROR }
    group_by: service
    aggregate: { func: count }
    having: { gt: 20 }
    emit: console
`)
	rules, err := rule.LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("got %d rules, want 1", len(rules))
	}
	r := rules[0]
	if r.Name != "error-spike" || r.WindowSize != 5*time.Minute {
		t.Errorf("name/window = %q/%s", r.Name, r.WindowSize)
	}
	if r.Filter.Field != "level" || r.Filter.Eq != "ERROR" {
		t.Errorf("filter = %+v", r.Filter)
	}
	if r.AggFunc != rule.AggCount || r.Having.Op != rule.OpGT || r.Having.Value != 20 {
		t.Errorf("agg/having = %s / %+v", r.AggFunc, r.Having)
	}
}

func TestLoadFile_SumRequiresField(t *testing.T) {
	path := writeConfig(t, `
rules:
  - name: bytes-sum
    aggregate: { func: sum }
    having: { gt: 1000 }
`)
	if _, err := rule.LoadFile(path); err == nil {
		t.Fatal("expected error for sum without aggregate.field")
	}
}

func TestLoadFile_RejectsUnknownField(t *testing.T) {
	path := writeConfig(t, `
rules:
  - name: typo
    aggregate: { func: count }
    having: { gt: 1 }
    threshhold: 5
`)
	if _, err := rule.LoadFile(path); err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestLoadFile_BadDuration(t *testing.T) {
	path := writeConfig(t, `
rules:
  - name: bad-window
    window: { size: 5minutes }
    aggregate: { func: count }
    having: { gt: 1 }
`)
	if _, err := rule.LoadFile(path); err == nil {
		t.Fatal("expected error for invalid window.size")
	}
}

func TestLoadFile_ArbitraryGroupBy(t *testing.T) {
	// Since #17 any field (a Fields key here) is a valid group_by.
	path := writeConfig(t, `
rules:
  - name: by-host
    group_by: host
    aggregate: { func: count }
    having: { gt: 1 }
`)
	rules, err := rule.LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if len(rules[0].GroupBy) != 1 || rules[0].GroupBy[0] != "host" {
		t.Fatalf("group_by = %v, want [host]", rules[0].GroupBy)
	}
}

func TestLoadFile_MultiGroupBy(t *testing.T) {
	path := writeConfig(t, `
rules:
  - name: multi-group
    group_by: [service, region]
    aggregate: { func: count }
    having: { gt: 1 }
`)
	rules, err := rule.LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if len(rules[0].GroupBy) != 2 || rules[0].GroupBy[0] != "service" || rules[0].GroupBy[1] != "region" {
		t.Fatalf("group_by = %v, want [service region]", rules[0].GroupBy)
	}
}

func TestLoadFile_SingleGroupByNormalized(t *testing.T) {
	// string and single-element array must produce identical []string.
	pathStr := writeConfig(t, `
rules:
  - name: str
    group_by: host
    aggregate: { func: count }
    having: { gt: 1 }
`)
	pathArr := writeConfig(t, `
rules:
  - name: arr
    group_by: [host]
    aggregate: { func: count }
    having: { gt: 1 }
`)
	rStr, _ := rule.LoadFile(pathStr)
	rArr, _ := rule.LoadFile(pathArr)
	if len(rStr[0].GroupBy) != 1 || rStr[0].GroupBy[0] != "host" {
		t.Fatalf("string form: %v", rStr[0].GroupBy)
	}
	if len(rArr[0].GroupBy) != 1 || rArr[0].GroupBy[0] != "host" {
		t.Fatalf("array form: %v", rArr[0].GroupBy)
	}
}

func TestLoadFile_HavingMustHaveOneOperator(t *testing.T) {
	path := writeConfig(t, `
rules:
  - name: two-ops
    aggregate: { func: count }
    having: { gt: 1, lt: 10 }
`)
	if _, err := rule.LoadFile(path); err == nil {
		t.Fatal("expected error for multi-operator having")
	}
}

func TestLoadFile_EmptyRules(t *testing.T) {
	path := writeConfig(t, "rules: []\n")
	if _, err := rule.LoadFile(path); err == nil {
		t.Fatal("expected error for empty rules")
	}
}

func TestLoadConfig_WithWebhookNotify(t *testing.T) {
	path := writeConfig(t, `
rules:
  - name: error-spike
    aggregate: { func: count }
    having: { gt: 1 }
notify:
  - type: console
  - type: webhook
    url: https://hooks.example.com/abc
    headers:
      Content-Type: application/json
    template: '{"text":"{{.RuleName}}"}'
`)
	cfg, err := rule.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.Rules) != 1 {
		t.Fatalf("got %d rules, want 1", len(cfg.Rules))
	}
	if len(cfg.Notify) != 2 {
		t.Fatalf("got %d notify entries, want 2", len(cfg.Notify))
	}
	wh := cfg.Notify[1]
	if wh.Type != "webhook" || wh.URL != "https://hooks.example.com/abc" {
		t.Errorf("notify[1] = %+v", wh)
	}
}

func TestLoadConfig_WebhookMissingURL(t *testing.T) {
	path := writeConfig(t, `
rules:
  - name: r
    aggregate: { func: count }
    having: { gt: 1 }
notify:
  - type: webhook
`)
	if _, err := rule.LoadConfig(path); err == nil {
		t.Fatal("expected error for webhook without url")
	}
}

func TestLoadConfig_UnknownNotifyType(t *testing.T) {
	path := writeConfig(t, `
rules:
  - name: r
    aggregate: { func: count }
    having: { gt: 1 }
notify:
  - type: slack
    url: https://example.com
`)
	if _, err := rule.LoadConfig(path); err == nil {
		t.Fatal("expected error for unknown notify type")
	}
}
