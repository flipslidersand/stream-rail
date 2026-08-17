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
	"github.com/flipslidersand/stream-rail/internal/store"
	"github.com/flipslidersand/stream-rail/internal/window"
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
	var configPath string
	var dataDir string
	run := &cobra.Command{
		Use:   "run",
		Short: "Start the stream processing engine",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServer(addr, windowSize, threshold, configPath, dataDir)
		},
	}
	run.Flags().StringVar(&addr, "addr", ":8080", "listen address")
	run.Flags().DurationVar(&windowSize, "window", 5*time.Minute, "default tumbling window size (rules without window.size)")
	run.Flags().Float64Var(&threshold, "threshold", 20, "built-in error-spike threshold (used when --config is unset)")
	run.Flags().StringVar(&configPath, "config", "", "path to rules.yaml (falls back to built-in error-spike rule if unset)")
	run.Flags().StringVar(&dataDir, "data", "", "BadgerDB directory for window state persistence (empty = in-memory only)")
	root.AddCommand(run)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServer(addr string, windowSize time.Duration, threshold float64, configPath, dataDir string) error {
	rules, err := loadRules(configPath, threshold)
	if err != nil {
		return err
	}

	var storeFactory window.StoreFactory
	if dataDir != "" {
		db, err := store.Open(dataDir)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()
		storeFactory = db
		fmt.Printf("persisting window state to %s\n", dataDir)
	}

	ch := make(chan model.Event, 1024)
	ing := ingester.NewHTTPIngester(ch)
	eng := engine.New(ch, windowSize, rules, nil, storeFactory)

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

	fmt.Printf("streamrail listening on %s (%d rule(s))\n", addr, len(rules))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// loadRules returns rules from configPath, or the built-in error-spike rule
// when configPath is empty.
func loadRules(configPath string, threshold float64) ([]rule.Rule, error) {
	if configPath != "" {
		rules, err := rule.LoadFile(configPath)
		if err != nil {
			return nil, err
		}
		fmt.Printf("loaded %d rule(s) from %s\n", len(rules), configPath)
		return rules, nil
	}
	return []rule.Rule{{
		Name:    "error-spike",
		Filter:  rule.Filter{Field: "level", Eq: "ERROR"},
		GroupBy: "service",
		AggFunc: rule.AggCount,
		Having:  rule.Having{Op: rule.OpGT, Value: threshold},
		Emit:    "console",
	}}, nil
}
