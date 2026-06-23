package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "streamrail",
		Short: "Real-time stream processing engine",
	}

	root.AddCommand(&cobra.Command{
		Use:   "run",
		Short: "Start the stream processing engine",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("streamrail — not yet implemented")
			return nil
		},
	})

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
