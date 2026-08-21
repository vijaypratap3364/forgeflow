// Command forgeflow starts the ForgeFlow HTTP API.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vijaypratap3364/forgeflow/internal/app"
)

func main() {
	config, err := app.ConfigFromEnv()
	if err != nil {
		log.Printf("configure ForgeFlow: %v", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, config, os.Stdout); err != nil {
		log.Printf("run ForgeFlow: %v", err)
		os.Exit(1)
	}
}
