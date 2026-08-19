package ingester

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// HTTPIngester は POST /events で受け取ったイベントを ch に投入する。
// HTTP には ack の概念がないため、封筒の Ack は nil で送る（#19）。
type HTTPIngester struct {
	ch            chan<- model.Envelope
	eventsCounter prometheus.Counter // optional; nil = no-op
}

func NewHTTPIngester(ch chan<- model.Envelope) *HTTPIngester {
	return &HTTPIngester{ch: ch}
}

// WithEventsCounter attaches a Prometheus counter incremented on each accepted
// event (#28). Call before serving requests.
func (h *HTTPIngester) WithEventsCounter(c prometheus.Counter) *HTTPIngester {
	h.eventsCounter = c
	return h
}

func (h *HTTPIngester) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var ev model.Event
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	select {
	case h.ch <- model.Envelope{Event: ev}:
		if h.eventsCounter != nil {
			h.eventsCounter.Inc()
		}
		w.WriteHeader(http.StatusAccepted)
	default:
		http.Error(w, "channel full", http.StatusServiceUnavailable)
	}
}
