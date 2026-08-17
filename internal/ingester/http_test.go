package ingester

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flipslidersand/stream-rail/internal/event"
)

func newRequest(t *testing.T, method, body string) *http.Request {
	t.Helper()
	return httptest.NewRequest(method, "/events", strings.NewReader(body))
}

func TestHandleEvents_Accepted(t *testing.T) {
	eventCh := make(chan event.Event, 1)
	ing := NewHTTP(eventCh, nil)

	rec := httptest.NewRecorder()
	ing.Handler().ServeHTTP(rec, newRequest(t, http.MethodPost,
		`{"service":"api","level":"ERROR","ts":1718000000}`))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	select {
	case ev := <-eventCh:
		if ev.Service != "api" || ev.Level != "ERROR" || ev.Timestamp != 1718000000 {
			t.Fatalf("unexpected event: %+v", ev)
		}
	default:
		t.Fatal("expected event on channel")
	}
}

func TestHandleEvents_Backpressure(t *testing.T) {
	eventCh := make(chan event.Event) // unbuffered, no consumer → always full
	ing := NewHTTP(eventCh, nil)

	rec := httptest.NewRecorder()
	ing.Handler().ServeHTTP(rec, newRequest(t, http.MethodPost,
		`{"service":"api","level":"ERROR","ts":1}`))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
}

func TestHandleEvents_BadJSON(t *testing.T) {
	eventCh := make(chan event.Event, 1)
	ing := NewHTTP(eventCh, nil)

	rec := httptest.NewRecorder()
	ing.Handler().ServeHTTP(rec, newRequest(t, http.MethodPost, `{not json`))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleEvents_MethodNotAllowed(t *testing.T) {
	eventCh := make(chan event.Event, 1)
	ing := NewHTTP(eventCh, nil)

	rec := httptest.NewRecorder()
	ing.Handler().ServeHTTP(rec, newRequest(t, http.MethodGet, ""))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
