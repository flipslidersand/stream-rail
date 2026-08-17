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
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "streamrail",
		Short: "Real-time stream processing engine",
	}

	var addr string
	var windowSize time.Duration
	run := &cobra.Command{
		Use:   "run",
		Short: "Start the stream processing engine",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServer(addr, windowSize)
		},
	}
	run.Flags().StringVar(&addr, "addr", ":8080", "listen address")
	run.Flags().DurationVar(&windowSize, "window", 5*time.Minute, "tumbling window size")
	root.AddCommand(run)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServer(addr string, windowSize time.Duration) error {
	ch := make(chan model.Event, 1024)

	ing := ingester.NewHTTPIngester(ch)
	eng := engine.New(ch, windowSize)

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
