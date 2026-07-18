package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

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
	run := &cobra.Command{
		Use:   "run",
		Short: "Start the stream processing engine",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServer(addr)
		},
	}
	run.Flags().StringVar(&addr, "addr", ":8080", "listen address")
	root.AddCommand(run)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServer(addr string) error {
	ch := make(chan model.Event, 1024)

	ing := ingester.NewHTTPIngester(ch)
	eng := engine.New(ch)

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
