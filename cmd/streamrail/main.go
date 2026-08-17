package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/flipslidersand/stream-rail/internal/event"
	"github.com/flipslidersand/stream-rail/internal/ingester"
)

func main() {
	root := &cobra.Command{
		Use:   "streamrail",
		Short: "Real-time stream processing engine",
	}

	var addr string
	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Start the stream processing engine",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd.Context(), addr)
		},
	}
	runCmd.Flags().StringVar(&addr, "addr", ":8080", "HTTP listen address for /events")
	root.AddCommand(runCmd)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, addr string) error {
	log, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer func() { _ = log.Sync() }()

	// Phase 1 pipeline stub: events land on eventCh and a single consumer
	// drains them. Later phases replace this consumer with the window manager.
	eventCh := make(chan event.Event, ingester.DefaultEventBuffer)
	go drain(ctx, eventCh, log)

	ing := ingester.NewHTTP(eventCh, log)
	srv := &http.Server{
		Addr:              addr,
		Handler:           ing.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("streamrail listening", zap.String("addr", addr))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	log.Info("streamrail stopped")
	return nil
}

// drain consumes events until the context is cancelled. In Phase 1 it only
// logs receipt so the /events → channel path is observable.
func drain(ctx context.Context, eventCh <-chan event.Event, log *zap.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-eventCh:
			log.Info("event received",
				zap.String("service", ev.Service),
				zap.String("level", ev.Level),
				zap.Int64("ts", ev.Timestamp))
		}
	}
}
