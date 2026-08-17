package engine

import (
	"context"
	"fmt"
	"io"
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
// Rules may declare different window sizes, so the engine runs one window
// Manager per distinct size and fans incoming events out to each. Every closed
// batch is evaluated only against the rules that share its window size.
type Engine struct {
	in            <-chan model.Event
	defaultWindow time.Duration
	lateness      time.Duration
	rules         []rule.Rule
	console       *notifier.Console
	store         window.StoreFactory // optional; nil disables persistence
}

// New builds an Engine reading from in. defaultWindow is applied to rules that
// don't declare their own window size (defaults to 5 minutes). out receives
// alert output (nil = os.Stdout). store may be nil to disable persistence.
func New(in <-chan model.Event, defaultWindow time.Duration, rules []rule.Rule, out io.Writer, store window.StoreFactory) *Engine {
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

// taggedBatch carries a closed window together with the rules that apply to it.
type taggedBatch struct {
	batch window.Batch
	rules []rule.Rule
}

// windowStream is one Manager's input channel plus the rules it serves.
type windowStream struct {
	in chan model.Event
}

// Run starts a window Manager per distinct window size, fans events out to all
// of them, and evaluates each closed batch against the matching rules.
func (e *Engine) Run(ctx context.Context) error {
	if len(e.rules) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}

	bySize := map[time.Duration][]rule.Rule{}
	for _, r := range e.rules {
		size := r.WindowSize
		if size <= 0 {
			size = e.defaultWindow
		}
		bySize[size] = append(bySize[size], r)
	}

	batchCh := make(chan taggedBatch, window.DefaultBatchBuffer)
	streams := make([]windowStream, 0, len(bySize))

	for size, rules := range bySize {
		in := make(chan model.Event, 1024)
		out := make(chan window.Batch, window.DefaultBatchBuffer)
		mgr := window.NewManager(size, in, out, nil, nil)
		mgr.SetLateness(e.lateness)
		rules := rules // capture per iteration

		if e.store != nil {
			mgr.SetStore(e.store.Buckets(size.String()))
			if n, err := mgr.Restore(); err != nil {
				return fmt.Errorf("restore windows (size=%s): %w", size, err)
			} else if n > 0 {
				fmt.Printf("[store] restored %d open window(s) for size=%s\n", n, size)
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

// fanOut copies each incoming event to every window stream.
func (e *Engine) fanOut(ctx context.Context, streams []windowStream) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-e.in:
			if !ok {
				return
			}
			for _, s := range streams {
				select {
				case s.in <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}
