// Package window implements tumbling (fixed-size, non-overlapping) time
// windows. Events are bucketed by their event-time truncated to the window
// size; when a window's end passes the processing clock it is flushed as a
// WindowBatch. See docs/implementation-guide.md Phase 2 and docs/data-model.md.
package window

import (
	"context"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// DefaultBatchBuffer is the recommended capacity for the batch channel
// (see docs/data-model.md).
const DefaultBatchBuffer = 1_000

// Key identifies a single window bucket. RuleName is populated once rule
// evaluation lands in Phase 3; Phase 2 groups purely by GroupKey + WindowStart.
type Key struct {
	RuleName    string
	GroupKey    string
	WindowStart time.Time
}

// Bucket accumulates the events that fall into one window.
type Bucket struct {
	Key    Key
	Events []model.Event
	Count  int64
}

// Batch is a closed window emitted downstream to the aggregator.
type Batch struct {
	Key       Key
	WindowEnd time.Time
	Events    []model.Event
	Count     int64
}

// GroupFunc derives the group-by key for an event. Phase 3 will build this
// from a rule's group_by field; Phase 2 defaults to grouping by service.
type GroupFunc func(model.Event) string

// GroupByService is the default grouping used until rules are wired in.
func GroupByService(ev model.Event) string { return ev.Service }

// Manager buckets incoming events into tumbling windows and emits closed
// windows on out. It is single-goroutine: all state is owned by Run.
type Manager struct {
	size    time.Duration
	in      <-chan model.Event
	out     chan<- Batch
	groupBy GroupFunc
	now     func() time.Time
	windows map[Key]*Bucket
}

// NewManager builds a window Manager. size is the window duration; in supplies
// events; out receives closed windows. groupBy/now may be nil (sensible
// defaults are used) — now is injectable so tests can control the clock.
func NewManager(size time.Duration, in <-chan model.Event, out chan<- Batch, groupBy GroupFunc, now func() time.Time) *Manager {
	if groupBy == nil {
		groupBy = GroupByService
	}
	if now == nil {
		now = time.Now
	}
	return &Manager{
		size:    size,
		in:      in,
		out:     out,
		groupBy: groupBy,
		now:     now,
		windows: make(map[Key]*Bucket),
	}
}

// Run consumes events until ctx is cancelled. A ticker fires every window size
// to flush windows whose end has passed the processing clock.
func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.size)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-m.in:
			if !ok {
				return nil
			}
			m.add(ev)
		case <-ticker.C:
			m.closeExpired(ctx, m.now())
		}
	}
}

// add places an event into its window bucket, creating the bucket on demand.
// The window start is normalized with Truncate so boundaries are stable
// regardless of when the event arrives.
func (m *Manager) add(ev model.Event) {
	start := time.Unix(ev.Timestamp, 0).UTC().Truncate(m.size)
	key := Key{GroupKey: m.groupBy(ev), WindowStart: start}
	b := m.windows[key]
	if b == nil {
		b = &Bucket{Key: key}
		m.windows[key] = b
	}
	b.Events = append(b.Events, ev)
	b.Count++
}

// closeExpired flushes every window whose end (start+size) is at or before now,
// emitting it as a Batch and removing it from the active set.
func (m *Manager) closeExpired(ctx context.Context, now time.Time) {
	for key, b := range m.windows {
		end := key.WindowStart.Add(m.size)
		if now.Before(end) {
			continue // window still open
		}
		batch := Batch{Key: b.Key, WindowEnd: end, Events: b.Events, Count: b.Count}
		select {
		case m.out <- batch:
			delete(m.windows, key)
		case <-ctx.Done():
			return // shutting down; leftover windows are dropped in Phase 2
		}
	}
}
