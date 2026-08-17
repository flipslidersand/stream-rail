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

// runConfig holds the resolved flags for the run command.
type runConfig struct {
	addr        string
	windowSize  time.Duration
	threshold   float64
	configPath  string
	dataDir     string
	natsURL     string
	natsSubject string
	lateness    time.Duration
}

func main() {
	root := &cobra.Command{
		Use:   "streamrail",
		Short: "Real-time stream processing engine",
	}

	var cfg runConfig
	run := &cobra.Command{
		Use:   "run",
		Short: "Start the stream processing engine",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServer(cfg)
		},
	}
	run.Flags().StringVar(&cfg.addr, "addr", ":8080", "listen address")
	run.Flags().DurationVar(&cfg.windowSize, "window", 5*time.Minute, "default tumbling window size (rules without window.size)")
	run.Flags().Float64Var(&cfg.threshold, "threshold", 20, "built-in error-spike threshold (used when --config is unset)")
	run.Flags().StringVar(&cfg.configPath, "config", "", "path to rules.yaml (falls back to built-in error-spike rule if unset)")
	run.Flags().StringVar(&cfg.dataDir, "data", "", "BadgerDB directory for window state persistence (empty = in-memory only)")
	run.Flags().StringVar(&cfg.natsURL, "nats", "", "NATS server URL for JetStream ingestion (e.g. nats://localhost:4222; empty = HTTP only)")
	run.Flags().StringVar(&cfg.natsSubject, "nats-subject", "application_logs", "NATS JetStream subject/stream to consume")
	run.Flags().DurationVar(&cfg.lateness, "lateness", 0, "allowed lateness for late-event correction (0 = disabled)")
	root.AddCommand(run)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServer(cfg runConfig) error {
	rules, err := loadRules(cfg.configPath, cfg.threshold)
	if err != nil {
		return err
	}

	var storeFactory window.StoreFactory
	if cfg.dataDir != "" {
		db, err := store.Open(cfg.dataDir)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()
		storeFactory = db
		fmt.Printf("persisting window state to %s\n", cfg.dataDir)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ch := make(chan model.Event, 1024)
	ing := ingester.NewHTTPIngester(ch)
	eng := engine.New(ch, cfg.windowSize, rules, nil, storeFactory).WithLateness(cfg.lateness)

	// Optional NATS JetStream ingester, feeding the same event channel.
	if cfg.natsURL != "" {
		ni := ingester.NewNATS(cfg.natsURL, cfg.natsSubject, ch)
		if err := ni.Start(ctx); err != nil {
			return err
		}
		defer ni.Stop()
		fmt.Printf("consuming NATS JetStream %s from %s\n", cfg.natsSubject, cfg.natsURL)
	}

	mux := http.NewServeMux()
	mux.Handle("/events", ing)
	srv := &http.Server{Addr: cfg.addr, Handler: mux}

	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	go func() {
		if err := eng.Run(ctx); err != nil && err != context.Canceled {
			fmt.Fprintf(os.Stderr, "engine error: %v\n", err)
		}
	}()

	fmt.Printf("streamrail listening on %s (%d rule(s))\n", cfg.addr, len(rules))
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
