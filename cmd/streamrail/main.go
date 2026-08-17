package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flipslidersand/stream-rail/internal/engine"
	"github.com/flipslidersand/stream-rail/internal/ingester"
	"github.com/flipslidersand/stream-rail/internal/model"
	"github.com/flipslidersand/stream-rail/internal/rule"
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "streamrail",
		Short: "Real-time stream processing engine",
	}

	var addr string
	var windowSize time.Duration
	var threshold float64
	run := &cobra.Command{
		Use:   "run",
		Short: "Start the stream processing engine",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServer(addr, windowSize, threshold)
		},
	}
	run.Flags().StringVar(&addr, "addr", ":8080", "listen address")
	run.Flags().DurationVar(&windowSize, "window", 5*time.Minute, "tumbling window size")
	run.Flags().Float64Var(&threshold, "threshold", 20, "error-spike alert threshold (count > N)")
	root.AddCommand(run)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServer(addr string, windowSize time.Duration, threshold float64) error {
	ch := make(chan model.Event, 1024)

	// Phase 3 ships a single built-in rule. Phase-later work loads these from
	// rules.yaml (see docs/spec.md).
	rules := []rule.Rule{{
		Name:    "error-spike",
		Filter:  rule.Filter{Field: "level", Eq: "ERROR"},
		GroupBy: "service",
		AggFunc: rule.AggCount,
		Having:  rule.Having{Op: rule.OpGT, Value: threshold},
		Emit:    "console",
	}}

	ing := ingester.NewHTTPIngester(ch)
	eng := engine.New(ch, windowSize, rules, nil)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mux := http.NewServeMux()
	mux.Handle("/events", ing)

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	go func() {
		if err := eng.Run(ctx); err != nil && err != context.Canceled {
			fmt.Fprintf(os.Stderr, "engine error: %v\n", err)
		}
	}()

	fmt.Printf("streamrail listening on %s\n", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
