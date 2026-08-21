package window

import (
	"context"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// SlidingManager groups events into overlapping sliding windows.
// Each event belongs to size/slide windows simultaneously:
// the window starting at ts.Truncate(slide), and size/slide-1 earlier windows.
// Windows close when the watermark (max event ts) passes their end, with a
// processing-time fallback so idle streams still flush.
// Persistence and late-event correction are not supported in this version.
type SlidingManager struct {
	size    time.Duration
	slide   time.Duration
	count   int // size / slide — how many windows each event touches
	in      <-chan model.Envelope
	out     chan<- Batch
	groupBy GroupFunc
	now     func() time.Time
	windows map[Key]*Bucket
	closedK map[Key]struct{} // keys closed this run; prevents re-opening
	maxTS   int64
}

// NewSlidingManager builds a SlidingManager. size is the window duration; slide
// is how often a new window starts (slide <= size). groupBy and now may be nil
// (sensible defaults are applied).
func NewSlidingManager(size, slide time.Duration, in <-chan model.Envelope, out chan<- Batch, groupBy GroupFunc, now func() time.Time) *SlidingManager {
	if groupBy == nil {
		groupBy = GroupByService
	}
	if now == nil {
		now = time.Now
	}
	return &SlidingManager{
		size:    size,
		slide:   slide,
		count:   int(size / slide),
		in:      in,
		out:     out,
		groupBy: groupBy,
		now:     now,
		windows: make(map[Key]*Bucket),
		closedK: make(map[Key]struct{}),
	}
}

// Run consumes events until ctx is cancelled. A ticker fires every slide
// interval to flush windows whose end has passed the watermark or wall clock.
func (m *SlidingManager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.slide)
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
			if env.Ack != nil {
				env.Ack()
			}
		case <-ticker.C:
			m.closeExpired(ctx, m.now())
		}
	}
}

// watermarkUnix is the event-time watermark: the max event timestamp seen.
func (m *SlidingManager) watermarkUnix() int64 { return m.maxTS }

// add places an event into every sliding window it belongs to.
// An event at ts maps to windows starting at:
//
//	ts.Truncate(slide), ts.Truncate(slide)-slide, …, ts.Truncate(slide)-(count-1)*slide
func (m *SlidingManager) add(ev model.Event) {
	if ev.Timestamp > m.maxTS {
		m.maxTS = ev.Timestamp
	}

	ts := time.Unix(ev.Timestamp, 0).UTC()
	base := ts.Truncate(m.slide)
	gk := m.groupBy(ev)

	for i := 0; i < m.count; i++ {
		start := base.Add(-time.Duration(i) * m.slide)
		end := start.Add(m.size)
		// Skip windows whose end is strictly before the watermark — they
		// will never receive new events anyway (the watermark has passed them).
		if end.Unix() < m.watermarkUnix() {
			continue
		}
		key := Key{GroupKey: gk, WindowStart: start}
		if _, done := m.closedK[key]; done {
			continue // do not re-open a window we already flushed
		}
		if b := m.windows[key]; b != nil {
			b.Events = append(b.Events, ev)
			b.Count++
		} else {
			m.windows[key] = &Bucket{Key: key, Events: []model.Event{ev}, Count: 1}
		}
	}
}

// closeExpired flushes windows whose end is at or before the watermark (event-
// time) or whose end has passed the wall clock (idle-stream fallback).
func (m *SlidingManager) closeExpired(ctx context.Context, now time.Time) {
	wm := m.watermarkUnix()
	for key, b := range m.windows {
		end := key.WindowStart.Add(m.size)
		if end.Unix() > wm && now.Before(end) {
			continue // still open
		}
		batch := Batch{Key: b.Key, WindowEnd: end, Events: b.Events, Count: b.Count}
		select {
		case m.out <- batch:
			delete(m.windows, key)
			m.closedK[key] = struct{}{}
		case <-ctx.Done():
			return
		}
	}
}
