package ingester_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flipslidersand/stream-rail/internal/ingester"
	"github.com/flipslidersand/stream-rail/internal/model"
)

func TestHTTPIngester_AcceptsValidEvent(t *testing.T) {
	ch := make(chan model.Event, 1)
	handler := ingester.NewHTTPIngester(ch)

	body := `{"service":"api","level":"ERROR","ts":1718000000}`
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d", w.Code)
	}

	select {
	case ev := <-ch:
		if ev.Service != "api" {
			t.Errorf("service: want api, got %s", ev.Service)
		}
		if ev.Level != "ERROR" {
			t.Errorf("level: want ERROR, got %s", ev.Level)
		}
		if ev.Timestamp != 1718000000 {
			t.Errorf("ts: want 1718000000, got %d", ev.Timestamp)
		}
	default:
		t.Fatal("no event in channel")
	}
}

func TestHTTPIngester_RejectsInvalidJSON(t *testing.T) {
	ch := make(chan model.Event, 1)
	handler := ingester.NewHTTPIngester(ch)

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString("{bad json}"))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", w.Code)
	}
}

func TestHTTPIngester_RejectsNonPost(t *testing.T) {
	ch := make(chan model.Event, 1)
	handler := ingester.NewHTTPIngester(ch)

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", w.Code)
	}
}

func TestHTTPIngester_FullChannelReturns503(t *testing.T) {
	ch := make(chan model.Event, 0) // unbuffered = full immediately
	handler := ingester.NewHTTPIngester(ch)

	body := `{"service":"x","level":"INFO","ts":1}`
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", w.Code)
	}
}
