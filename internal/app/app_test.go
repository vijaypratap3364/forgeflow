package app

import (
	"bytes"
	"testing"
)

func TestRunWritesStartupMessage(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer

	if err := Run(&output); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	want := startupMessage + "\n"
	if got := output.String(); got != want {
		t.Fatalf("Run() output = %q, want %q", got, want)
	}
}
