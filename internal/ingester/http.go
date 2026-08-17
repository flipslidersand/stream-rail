// Package ingester receives events from external sources (HTTP in Phase 1,
// NATS JetStream in Phase 5) and pushes them onto the pipeline's event channel.
package ingester

import (
	"encoding/json"
	"net/http"

	"go.uber.org/zap"

	"github.com/flipslidersand/stream-rail/internal/event"
)

// DefaultEventBuffer is the recommended capacity for the event channel
// (see docs/data-model.md).
const DefaultEventBuffer = 10_000

// HTTPIngester exposes an HTTP endpoint that decodes JSON events and forwards
// them onto eventCh. When the channel is full it applies backpressure by
// returning 429 rather than blocking the request goroutine.
type HTTPIngester struct {
	eventCh chan<- event.Event
	log     *zap.Logger
}

// NewHTTP builds an ingester that writes accepted events to eventCh.
func NewHTTP(eventCh chan<- event.Event, log *zap.Logger) *HTTPIngester {
	if log == nil {
		log = zap.NewNop()
	}
	return &HTTPIngester{eventCh: eventCh, log: log}
}

// Handler returns the http.Handler serving POST /events.
func (i *HTTPIngester) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", i.handleEvents)
	return mux
}

func (i *HTTPIngester) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var ev event.Event
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)) // 1 MiB guard
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ev); err != nil {
		i.log.Debug("event decode failed", zap.Error(err))
		http.Error(w, "invalid event: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Backpressure: never block the request goroutine. If the pipeline can't
	// keep up, shed load with 429 so the client can retry/slow down.
	select {
	case i.eventCh <- ev:
		w.WriteHeader(http.StatusAccepted)
	default:
		i.log.Warn("event channel full, shedding load",
			zap.String("service", ev.Service), zap.String("level", ev.Level))
		http.Error(w, "event channel full", http.StatusTooManyRequests)
	}
}
