package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
	"github.com/flipslidersand/stream-rail/internal/window"
)

// Engine wires the pipeline stages together. Phase 2 adds tumbling-window
// bucketing between ingestion and (future) aggregation.
type Engine struct {
	in         <-chan model.Event
	windowSize time.Duration
}

// New builds an Engine reading from in. windowSize is the tumbling window
// duration; if zero it defaults to 5 minutes (the spec's example window).
func New(in <-chan model.Event, windowSize time.Duration) *Engine {
	if windowSize <= 0 {
		windowSize = 5 * time.Minute
	}
	return &Engine{in: in, windowSize: windowSize}
}

// Run drives the window manager and consumes closed windows until ctx is
// cancelled. Phase 3 replaces the batch logging with aggregation + rules.
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
			fmt.Printf("[window] closed group=%s start=%s end=%s count=%d\n",
				batch.Key.GroupKey,
				batch.Key.WindowStart.Format(time.RFC3339),
				batch.WindowEnd.Format(time.RFC3339),
				batch.Count)
		}
	}
}
