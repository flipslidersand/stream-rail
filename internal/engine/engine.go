package engine

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/flipslidersand/stream-rail/internal/aggregator"
	"github.com/flipslidersand/stream-rail/internal/model"
	"github.com/flipslidersand/stream-rail/internal/notifier"
	"github.com/flipslidersand/stream-rail/internal/rule"
	"github.com/flipslidersand/stream-rail/internal/window"
)

// Engine wires the pipeline stages together: ingestion → tumbling window →
// aggregate → HAVING evaluation → console notification.
//
// Rules may declare different window sizes and group-by fields, so the engine
// runs one window Manager per distinct (window size, group_by) combination and
// fans incoming events out to each. Every closed batch is evaluated only
// against the rules that share its window size and grouping.
type Engine struct {
	in            <-chan model.Envelope
	defaultWindow time.Duration
	lateness      time.Duration
	rules         []rule.Rule
	console       *notifier.Console
	store         window.StoreFactory // optional; nil disables persistence
}

// New builds an Engine reading from in. defaultWindow is applied to rules that
// don't declare their own window size (defaults to 5 minutes). out receives
// alert output (nil = os.Stdout). store may be nil to disable persistence.
func New(in <-chan model.Envelope, defaultWindow time.Duration, rules []rule.Rule, out io.Writer, store window.StoreFactory) *Engine {
	if defaultWindow <= 0 {
		defaultWindow = 5 * time.Minute
	}
	return &Engine{
		in:            in,
		defaultWindow: defaultWindow,
		rules:         rules,
		console:       notifier.NewConsole(out),
		store:         store,
	}
}

// WithLateness sets the allowed lateness for late-event correction (Phase 6).
func (e *Engine) WithLateness(d time.Duration) *Engine {
	e.lateness = d
	return e
}

// streamKey identifies one window Manager: rules sharing both a window size and
// a group_by field are served by the same Manager so their GroupKeys line up.
type streamKey struct {
	size    time.Duration
	groupBy string
}

// namespace is the persistence prefix for this stream. It includes the group_by
// field so managers of the same size but different grouping don't collide in the
// store.
func (k streamKey) namespace() string { return k.size.String() + "/" + k.groupBy }

// normalizeGroupBy returns a canonical group_by key string for use as a map key
// and persistence namespace. An empty or nil slice defaults to "service".
// Multiple fields are joined with "|" so distinct combinations map to distinct keys.
func normalizeGroupBy(fields []string) string {
	if len(fields) == 0 {
		return "service"
	}
	return strings.Join(fields, "|")
}

// groupFuncFor builds the window GroupFunc that extracts the group_by composite
// key from each event. Supports single and multi-field grouping (#26).
func groupFuncFor(fields []string) window.GroupFunc {
	return func(ev model.Event) string { return rule.GroupKey(ev, fields) }
}

// taggedBatch carries a closed window together with the rules that apply to it.
type taggedBatch struct {
	batch window.Batch
	rules []rule.Rule
}

// windowStream is one Manager's input channel plus the rules it serves.
type windowStream struct {
	in chan model.Envelope
}

// Run starts a window Manager per distinct window size, fans events out to all
// of them, and evaluates each closed batch against the matching rules.
func (e *Engine) Run(ctx context.Context) error {
	if len(e.rules) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}

	byGroup := map[streamKey][]rule.Rule{}
	for _, r := range e.rules {
		size := r.WindowSize
		if size <= 0 {
			size = e.defaultWindow
		}
		gk := streamKey{size: size, groupBy: normalizeGroupBy(r.GroupBy)}
		byGroup[gk] = append(byGroup[gk], r)
	}

	// groupFields holds the original []string per streamKey for GroupFunc construction.
	groupFields := map[streamKey][]string{}
	for _, r := range e.rules {
		size := r.WindowSize
		if size <= 0 {
			size = e.defaultWindow
		}
		gk := streamKey{size: size, groupBy: normalizeGroupBy(r.GroupBy)}
		if _, ok := groupFields[gk]; !ok {
			groupFields[gk] = r.GroupBy
		}
	}

	batchCh := make(chan taggedBatch, window.DefaultBatchBuffer)
	streams := make([]windowStream, 0, len(byGroup))

	for gk, rules := range byGroup {
		in := make(chan model.Envelope, 1024)
		out := make(chan window.Batch, window.DefaultBatchBuffer)
		mgr := window.NewManager(gk.size, in, out, groupFuncFor(groupFields[gk]), nil)
		mgr.SetLateness(e.lateness)
		rules := rules // capture per iteration

		if e.store != nil {
			mgr.SetStore(e.store.Buckets(gk.namespace()))
			if n, err := mgr.Restore(); err != nil {
				return fmt.Errorf("restore windows (%s): %w", gk.namespace(), err)
			} else if n > 0 {
				fmt.Printf("[store] restored %d open window(s) for %s\n", n, gk.namespace())
			}
		}

		go func() { _ = mgr.Run(ctx) }()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case b, ok := <-out:
					if !ok {
						return
					}
					select {
					case batchCh <- taggedBatch{batch: b, rules: rules}:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		streams = append(streams, windowStream{in: in})
	}

	go e.fanOut(ctx, streams)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tb := <-batchCh:
			for _, r := range tb.rules {
				res := aggregator.Aggregate(tb.batch, r)
				e.console.Emit(r, res)
			}
		}
	}
}

// fanOut copies each incoming envelope to every window stream. Because one event
// is added to every stream's window Manager, the upstream Ack must fire only
// once — after all managers have durably processed it (end-to-end at-least-once,
// #19). A per-event countdown wraps the original Ack: each stream's copy carries
// a child Ack that decrements the counter, and the last one to finish invokes the
// real Ack. On shutdown some copies may never be delivered, so the counter never
// reaches zero and the event is left unacked for redelivery — exactly the
// at-least-once contract.
func (e *Engine) fanOut(ctx context.Context, streams []windowStream) {
	for {
		select {
		case <-ctx.Done():
			return
		case env, ok := <-e.in:
			if !ok {
				return
			}
			done := ackBarrier(env.Ack, len(streams))
			for _, s := range streams {
				child := model.Envelope{Event: env.Event, Ack: done}
				select {
				case s.in <- child:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// ackBarrier returns a func that, once called n times, invokes ack exactly once.
// If ack is nil (e.g. HTTP ingestion) the returned func is a no-op. n is always
// >= 1 here because the engine runs at least one window Manager.
func ackBarrier(ack func(), n int) func() {
	if ack == nil {
		return func() {}
	}
	remaining := int32(n)
	return func() {
		if atomic.AddInt32(&remaining, -1) == 0 {
			ack()
		}
	}
}
