// Package metrics defines the Prometheus counters exposed by StreamRail.
// All metrics are registered on a single Registry so tests can use an isolated
// one rather than the global default.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds all StreamRail Prometheus counters.
type Metrics struct {
	// EventsTotal counts every event accepted into the pipeline.
	// Label source: "http" | "nats"
	EventsTotal *prometheus.CounterVec

	// DedupedTotal counts NATS messages skipped due to deduplication (#23).
	DedupedTotal prometheus.Counter

	// WindowsClosedTotal counts window batches flushed downstream.
	// Label corrected: "true" (late-event correction) | "false" (normal close).
	WindowsClosedTotal *prometheus.CounterVec

	// LateEventsTotal counts events that arrived after their window closed.
	LateEventsTotal prometheus.Counter
}

// New registers all metrics on reg and returns a Metrics instance.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamrail_events_total",
			Help: "Total number of events accepted into the pipeline.",
		}, []string{"source"}),

		DedupedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamrail_deduped_total",
			Help: "Total number of NATS messages skipped by deduplication.",
		}),

		WindowsClosedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamrail_windows_closed_total",
			Help: "Total number of tumbling windows flushed downstream.",
		}, []string{"corrected"}),

		LateEventsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamrail_late_events_total",
			Help: "Total number of events that arrived after their window closed.",
		}),
	}
	reg.MustRegister(
		m.EventsTotal,
		m.DedupedTotal,
		m.WindowsClosedTotal,
		m.LateEventsTotal,
	)
	return m
}
