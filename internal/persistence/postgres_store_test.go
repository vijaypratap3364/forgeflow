package persistence

import (
	"context"
	"errors"
	"testing"
)

func TestNewPostgresStoreRejectsNilPool(t *testing.T) {
	t.Parallel()

	if _, err := NewPostgresStore(nil); err == nil {
		t.Fatal("NewPostgresStore(nil) error = nil")
	}
}

func TestOpenPostgresStoreValidatesBeforeConnecting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ctx  context.Context
		dsn  string
		want error
	}{
		{name: "nil context", dsn: "postgres://unused", want: errors.New("non-nil error")},
		{name: "empty DSN", ctx: context.Background(), want: errors.New("non-nil error")},
		{name: "canceled context", ctx: canceledContext(), dsn: "postgres://unused", want: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := OpenPostgresStore(test.ctx, test.dsn)
			if err == nil {
				t.Fatal("OpenPostgresStore() error = nil")
			}
			if errors.Is(test.want, context.Canceled) && !errors.Is(err, context.Canceled) {
				t.Fatalf("OpenPostgresStore() error = %v, want context.Canceled", err)
			}
		})
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
