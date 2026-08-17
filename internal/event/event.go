// Package event defines the core data structures that flow through the
// StreamRail pipeline. See docs/data-model.md for the full model.
package event

// Event is a single record received by an ingester (HTTP or NATS).
type Event struct {
	Service   string         `json:"service"`
	Level     string         `json:"level"`
	Timestamp int64          `json:"ts"` // Unix 秒
	Fields    map[string]any `json:"fields,omitempty"`
}
