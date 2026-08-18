// Package window implements tumbling (fixed-size, non-overlapping) time
// windows. Events are bucketed by their event-time truncated to the window
// size. A window closes when the watermark (max event time minus allowed
// lateness) passes its end, with a processing-time fallback so idle streams
// still flush. Recently-closed windows are retained for the lateness horizon so
// late events can reopen and re-emit them (Phase 6).
// See docs/implementation-guide.md Phases 2 & 6 and docs/data-model.md.
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

// Batch is a closed window emitted downstream to the aggregator. Corrected is
// true when the batch is a re-emission triggered by a late event landing in an
// already-closed window.
type Batch struct {
	Key       Key
	WindowEnd time.Time
	Events    []model.Event
	Count     int64
	Corrected bool
}

// GroupFunc derives the group-by key for an event. Phase 3 will build this
// from a rule's group_by field; Phase 2 defaults to grouping by service.
type GroupFunc func(model.Event) string

// GroupByService is the default grouping used until rules are wired in.
func GroupByService(ev model.Event) string { return ev.Service }

// Store persists open window buckets so in-progress aggregation survives a
// restart (Phase 4). Implemented by internal/store on top of BadgerDB.
type Store interface {
	Save(b *Bucket) error        // upsert an open bucket
	Delete(k Key) error          // remove a closed/flushed bucket
	LoadAll() ([]*Bucket, error) // load all open buckets at startup
}

// StoreFactory produces a namespaced Store per window size, so buckets from
// managers of different sizes don't collide.
type StoreFactory interface {
	Buckets(prefix string) Store
}

// Manager buckets incoming events into tumbling windows and emits closed
// windows on out. It is single-goroutine: all state is owned by Run.
type Manager struct {
	size     time.Duration
	lateness time.Duration
	in       <-chan model.Envelope
	out      chan<- Batch
	groupBy  GroupFunc
	now      func() time.Time
	windows  map[Key]*Bucket // open windows
	closed   map[Key]*Bucket // recently closed, kept for late correction
	maxTS    int64           // max event timestamp seen (unix seconds)
	store    Store           // optional; nil disables persistence
}

// NewManager builds a window Manager. size is the window duration; in supplies
// event envelopes; out receives closed windows. groupBy/now may be nil (sensible
// defaults are used) — now is injectable so tests can control the clock.
func NewManager(size time.Duration, in <-chan model.Envelope, out chan<- Batch, groupBy GroupFunc, now func() time.Time) *Manager {
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
		closed:  make(map[Key]*Bucket),
	}
}

// SetStore attaches a persistence backend. Call before Run.
func (m *Manager) SetStore(s Store) { m.store = s }

// SetLateness configures the allowed lateness horizon. With lateness > 0 a
// window stays correctable for that long after it closes; late events landing
// in it re-open the window and emit a corrected batch. Default 0 disables
// correction (a closed window is dropped immediately).
func (m *Manager) SetLateness(d time.Duration) { m.lateness = d }

// Restore loads any open window buckets left over from a previous run, so
// in-progress aggregation resumes after a restart. Returns the count restored.
func (m *Manager) Restore() (int, error) {
	if m.store == nil {
		return 0, nil
	}
	buckets, err := m.store.LoadAll()
	if err != nil {
		return 0, err
	}
	for _, b := range buckets {
		m.windows[b.Key] = b
	}
	return len(buckets), nil
}

// Run consumes events until ctx is cancelled. A ticker fires every window size
// to flush windows whose end has passed the watermark or the processing clock.
func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.size)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case env, ok := <-m.in:
			if !ok {
				return nil
			}
			m.add(env.Event)
			// End-to-end ack (#19): the event is now durably held (persisted if a
			// store is attached), so it is safe to acknowledge upstream.
			if env.Ack != nil {
				env.Ack()
			}
		case <-ticker.C:
			m.closeExpired(ctx, m.now())
		}
	}
}

// watermarkUnix is the event-time watermark: the max event timestamp seen minus
// the allowed lateness, in unix seconds. Windows ending at or before it are
// considered complete.
func (m *Manager) watermarkUnix() int64 {
	return m.maxTS - int64(m.lateness/time.Second)
}

func (m *Manager) windowEnd(k Key) time.Time { return k.WindowStart.Add(m.size) }

// add places an event into its window bucket. The window start is normalized
// with Truncate so boundaries are stable regardless of when the event arrives.
// A late event whose window has already closed re-opens that window and emits a
// corrected batch.
func (m *Manager) add(ev model.Event) {
	if ev.Timestamp > m.maxTS {
		m.maxTS = ev.Timestamp
	}
	start := time.Unix(ev.Timestamp, 0).UTC().Truncate(m.size)
	key := Key{GroupKey: m.groupBy(ev), WindowStart: start}

	switch {
	case m.windows[key] != nil:
		// Still-open window: normal accumulation.
		m.append(m.windows[key], ev)

	case m.closed[key] != nil:
		// Late event into a window we closed but still retain: correct it.
		b := m.closed[key]
		m.append(b, ev)
		m.emitBlocking(b, true)

	case m.windowEnd(key).Unix() <= m.watermarkUnix():
		// Late event for a window we never held open (already past watermark):
		// materialize it in the closed set and emit as a correction.
		b := &Bucket{Key: key}
		m.closed[key] = b
		m.append(b, ev)
		m.emitBlocking(b, true)

	default:
		// New open window.
		b := &Bucket{Key: key}
		m.windows[key] = b
		m.append(b, ev)
	}
}

// append adds an event to a bucket and persists it if a store is attached.
func (m *Manager) append(b *Bucket, ev model.Event) {
	b.Events = append(b.Events, ev)
	b.Count++
	if m.store != nil {
		_ = m.store.Save(b) // best-effort durability
	}
}

// emitBlocking sends a batch downstream. Used for late corrections outside the
// ticker path; guarded so a nil out channel (unit tests) is a no-op.
func (m *Manager) emitBlocking(b *Bucket, corrected bool) {
	if m.out == nil {
		return
	}
	m.out <- Batch{
		Key:       b.Key,
		WindowEnd: m.windowEnd(b.Key),
		Events:    b.Events,
		Count:     b.Count,
		Corrected: corrected,
	}
}

// closeExpired flushes open windows whose end is at or before the watermark
// (event-time) or has passed the processing clock by the lateness margin
// (idle-stream fallback). Closed windows are retained for the lateness horizon
// for late correction, then garbage-collected.
func (m *Manager) closeExpired(ctx context.Context, now time.Time) {
	wm := m.watermarkUnix()

	for key, b := range m.windows {
		end := m.windowEnd(key)
		closedByWatermark := end.Unix() <= wm
		closedByProcessing := !now.Before(end.Add(m.lateness))
		if !closedByWatermark && !closedByProcessing {
			continue // window still open
		}
		batch := Batch{Key: b.Key, WindowEnd: end, Events: b.Events, Count: b.Count}
		select {
		case m.out <- batch:
			delete(m.windows, key)
			m.closed[key] = b // retain for possible late correction
		case <-ctx.Done():
			return // shutting down; leftover windows stay persisted for resume
		}
	}

	m.gcClosed(now, wm)
}

// gcClosed drops closed windows once they fall outside the correction horizon
// (watermark, or processing clock, has advanced a full lateness past the end).
func (m *Manager) gcClosed(now time.Time, wm int64) {
	latenessSec := int64(m.lateness / time.Second)
	for key := range m.closed {
		end := m.windowEnd(key)
		gcByWatermark := wm >= end.Unix()+latenessSec
		gcByProcessing := !now.Before(end.Add(2 * m.lateness))
		if gcByWatermark || gcByProcessing {
			delete(m.closed, key)
			if m.store != nil {
				_ = m.store.Delete(key)
			}
		}
	}
}
