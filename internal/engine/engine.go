package engine

import (
	"context"
	"fmt"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// Engine はイベントチャンネルを読み続けるパイプライン骨組み。
// Phase 2 以降で集計・アラートロジックを追加する。
type Engine struct {
	in <-chan model.Event
}

func New(in <-chan model.Event) *Engine {
	return &Engine{in: in}
}

// Run はコンテキストがキャンセルされるまでイベントを消費する。
func (e *Engine) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-e.in:
			if !ok {
				return nil
			}
			// Phase 2: ウィンドウ集計・ルール評価をここに追加
			fmt.Printf("[engine] received: service=%s level=%s ts=%d\n",
				ev.Service, ev.Level, ev.Timestamp)
		}
	}
}
