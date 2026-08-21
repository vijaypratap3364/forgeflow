// Command forgeflow starts the ForgeFlow application.
package main

import (
	"log"
	"os"

	"github.com/vijaypratap3364/forgeflow/internal/app"
)

func main() {
	if err := app.Run(os.Stdout); err != nil {
		log.Printf("run ForgeFlow: %v", err)
		os.Exit(1)
	}
}
