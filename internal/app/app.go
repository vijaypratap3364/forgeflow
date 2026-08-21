// Package app contains the ForgeFlow application bootstrap.
package app

import (
	"fmt"
	"io"
)

const startupMessage = "ForgeFlow bootstrap is ready"

// Run starts the current minimal application and reports startup to output.
func Run(output io.Writer) error {
	if _, err := fmt.Fprintln(output, startupMessage); err != nil {
		return fmt.Errorf("write startup message: %w", err)
	}

	return nil
}
