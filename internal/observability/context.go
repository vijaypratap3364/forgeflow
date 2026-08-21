package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"go.opentelemetry.io/otel/trace"
)

type requestIDContextKey struct{}

// NewRequestID returns a cryptographically random request correlation ID.
func NewRequestID() (string, error) {
	identifier := make([]byte, 16)
	if _, err := rand.Read(identifier); err != nil {
		return "", err
	}
	return hex.EncodeToString(identifier), nil
}

// WithRequestID returns a context containing the server-generated request ID.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDContextKey{}, requestID)
}

// RequestID returns the request ID carried by ctx, if any.
func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	return requestID
}

// CorrelationAttributes returns slog-compatible correlation key/value pairs.
func CorrelationAttributes(ctx context.Context) []any {
	attributes := make([]any, 0, 6)
	if requestID := RequestID(ctx); requestID != "" {
		attributes = append(attributes, "request_id", requestID)
	}
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		attributes = append(
			attributes,
			"trace_id", spanContext.TraceID().String(),
			"span_id", spanContext.SpanID().String(),
		)
	}
	return attributes
}
