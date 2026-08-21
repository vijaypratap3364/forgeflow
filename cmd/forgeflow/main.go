// Command forgeflow starts the ForgeFlow HTTP API.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/vijaypratap3364/forgeflow/internal/app"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	config, err := app.ConfigFromEnv()
	if err != nil {
		logger.Error("configure ForgeFlow", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, config, os.Stdout); err != nil {
		logger.Error("run ForgeFlow", "error", err)
		os.Exit(1)
	}
}
