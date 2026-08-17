package engine

import (
	"context"
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
type Engine struct {
	in         <-chan model.Event
	windowSize time.Duration
	rules      []rule.Rule
	console    *notifier.Console
}

// New builds an Engine reading from in. windowSize is the tumbling window
// duration (defaults to 5 minutes). rules are evaluated against every closed
// window; out receives alert output (nil = os.Stdout).
func New(in <-chan model.Event, windowSize time.Duration, rules []rule.Rule, out io.Writer) *Engine {
	if windowSize <= 0 {
		windowSize = 5 * time.Minute
	}
	return &Engine{
		in:         in,
		windowSize: windowSize,
		rules:      rules,
		console:    notifier.NewConsole(out),
	}
}

// Run drives the window manager and evaluates each closed window against every
// rule, emitting alerts when a HAVING condition is satisfied.
func (e *Engine) Run(ctx context.Context) error {
	batchCh := make(chan window.Batch, window.DefaultBatchBuffer)
	mgr := window.NewManager(e.windowSize, e.in, batchCh, nil, nil)

	go func() {
		_ = mgr.Run(ctx)
		close(batchCh)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-batchCh:
			if !ok {
				return nil
			}
			for _, r := range e.rules {
				res := aggregator.Aggregate(batch, r)
				e.console.Emit(r, res)
			}
		}
	}
}
