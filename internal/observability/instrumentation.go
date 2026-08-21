// Package observability provides ForgeFlow's process-local logging, metrics,
// and tracing primitives without requiring an observability backend locally.
package observability

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Instrumentation groups explicitly injected observability dependencies.
// It does not mutate OpenTelemetry's global providers.
type Instrumentation struct {
	logger     *slog.Logger
	metrics    *Metrics
	provider   trace.TracerProvider
	propagator propagation.TextMapPropagator
}

// NewInstrumentation constructs an observability dependency bundle. Nil
// components are replaced with safe process-local defaults.
func NewInstrumentation(
	logger *slog.Logger,
	metrics *Metrics,
	provider trace.TracerProvider,
	propagator propagation.TextMapPropagator,
) *Instrumentation {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if metrics == nil {
		metrics = NewMetrics()
	}
	if provider == nil {
		provider = noop.NewTracerProvider()
	}
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)
	}
	return &Instrumentation{
		logger:     logger,
		metrics:    metrics,
		provider:   provider,
		propagator: propagator,
	}
}

// NewNoopInstrumentation returns silent logging, in-process metrics, W3C
// propagation, and a no-op tracer provider.
func NewNoopInstrumentation() *Instrumentation {
	return NewInstrumentation(nil, nil, nil, nil)
}

// Logger returns the configured structured logger.
func (instrumentation *Instrumentation) Logger() *slog.Logger {
	return instrumentation.logger
}

// Metrics returns the process-local metrics registry.
func (instrumentation *Instrumentation) Metrics() *Metrics {
	return instrumentation.metrics
}

// Tracer returns a tracer from the explicitly configured provider.
func (instrumentation *Instrumentation) Tracer(name string) trace.Tracer {
	return instrumentation.provider.Tracer(name)
}

// Propagator returns the configured message and HTTP context propagator.
func (instrumentation *Instrumentation) Propagator() propagation.TextMapPropagator {
	return instrumentation.propagator
}

// Log emits a structured record enriched with request and trace correlation
// fields from ctx. Callers should pass identifiers, not task inputs or tokens.
func (instrumentation *Instrumentation) Log(
	ctx context.Context,
	level slog.Level,
	message string,
	attributes ...any,
) {
	correlation := CorrelationAttributes(ctx)
	correlation = append(correlation, attributes...)
	instrumentation.logger.Log(ctx, level, message, correlation...)
}
