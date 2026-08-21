package execution

import (
	"errors"
	"testing"
)

func TestRetryableErrorClassification(t *testing.T) {
	t.Parallel()

	cause := errors.New("temporary dependency failure")
	marked := Retryable(cause)
	if !IsRetryable(marked) {
		t.Fatal("IsRetryable() = false for marked failure")
	}
	if !errors.Is(marked, cause) {
		t.Fatal("Retryable() did not preserve the underlying error")
	}
	if IsRetryable(cause) {
		t.Fatal("IsRetryable() = true for unmarked terminal failure")
	}
	if Retryable(nil) != nil {
		t.Fatal("Retryable(nil) did not return nil")
	}
}
