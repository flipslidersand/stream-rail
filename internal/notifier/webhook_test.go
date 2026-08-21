package notifier_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/aggregator"
	"github.com/flipslidersand/stream-rail/internal/notifier"
	"github.com/flipslidersand/stream-rail/internal/rule"
	"github.com/flipslidersand/stream-rail/internal/window"
)

func webhookResult(value float64) aggregator.Result {
	start := time.Date(2024, 6, 10, 10, 0, 0, 0, time.UTC)
	return aggregator.Result{
		Key:       window.Key{GroupKey: "api", WindowStart: start},
		WindowEnd: start.Add(5 * time.Minute),
		Count:     int64(value),
		Value:     value,
	}
}

var spikeRule = rule.Rule{
	Name:    "error-spike",
	GroupBy: []string{"service"},
	AggFunc: rule.AggCount,
	Having:  rule.Having{Op: rule.OpGT, Value: 20},
}

func TestWebhook_FiresOnThresholdExceeded(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	wh, err := notifier.NewWebhook(srv.URL, "", nil, "")
	if err != nil {
		t.Fatalf("NewWebhook: %v", err)
	}

	if fired := wh.Emit(spikeRule, webhookResult(21)); !fired {
		t.Fatal("expected alert to fire for 21 > 20")
	}
	if !strings.Contains(gotBody, `"rule":"error-spike"`) {
		t.Fatalf("body missing rule name: %q", gotBody)
	}
	if !strings.Contains(gotBody, `"group":"api"`) {
		t.Fatalf("body missing group key: %q", gotBody)
	}
}

func TestWebhook_SilentBelowThreshold(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	wh, _ := notifier.NewWebhook(srv.URL, "", nil, "")
	if fired := wh.Emit(spikeRule, webhookResult(20)); fired {
		t.Fatal("did not expect alert for 20 > 20")
	}
	if called {
		t.Fatal("did not expect HTTP request below threshold")
	}
}

func TestWebhook_CustomTemplate(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tmpl := `ALERT {{.RuleName}} count={{.Value}}`
	wh, err := notifier.NewWebhook(srv.URL, "", nil, tmpl)
	if err != nil {
		t.Fatalf("NewWebhook: %v", err)
	}

	wh.Emit(spikeRule, webhookResult(21))
	if gotBody != "ALERT error-spike count=21" {
		t.Fatalf("unexpected body: %q", gotBody)
	}
}

func TestWebhook_InvalidTemplate(t *testing.T) {
	_, err := notifier.NewWebhook("http://localhost", "", nil, "{{.Unclosed")
	if err == nil {
		t.Fatal("expected error for invalid template")
	}
}

func TestWebhook_CustomHeaders(t *testing.T) {
	var gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	headers := map[string]string{"Content-Type": "text/plain"}
	wh, _ := notifier.NewWebhook(srv.URL, "", headers, "hello")
	wh.Emit(spikeRule, webhookResult(21))

	if gotContentType != "text/plain" {
		t.Fatalf("Content-Type = %q, want text/plain", gotContentType)
	}
}
