package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const (
	// TraceExporterNone disables span recording while retaining propagation.
	TraceExporterNone = "none"
	// TraceExporterStdout writes completed spans to the process output.
	TraceExporterStdout = "stdout"
	// TraceExporterOTLPHTTP exports spans through OTLP over HTTP.
	TraceExporterOTLPHTTP = "otlp-http"
)

// TraceConfig selects an optional OpenTelemetry exporter.
type TraceConfig struct {
	Exporter    string
	ServiceName string
}

// Validate verifies tracing configuration without opening an exporter.
func (config TraceConfig) Validate() error {
	switch strings.ToLower(strings.TrimSpace(config.Exporter)) {
	case TraceExporterNone, TraceExporterStdout, TraceExporterOTLPHTTP:
	default:
		return fmt.Errorf("unsupported trace exporter %q: use none, stdout, or otlp-http", config.Exporter)
	}
	if strings.TrimSpace(config.ServiceName) == "" {
		return errors.New("OpenTelemetry service name must not be empty")
	}
	return nil
}

// OpenTracerProvider creates an explicitly owned tracer provider. The OTLP
// exporter honors the standard OTEL_EXPORTER_OTLP_* environment variables.
func OpenTracerProvider(
	ctx context.Context,
	config TraceConfig,
	output io.Writer,
) (trace.TracerProvider, func(context.Context) error, error) {
	if ctx == nil {
		return nil, nil, errors.New("open tracer provider: context must not be nil")
	}
	if output == nil {
		return nil, nil, errors.New("open tracer provider: output must not be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, nil, err
	}

	exporterName := strings.ToLower(strings.TrimSpace(config.Exporter))
	if exporterName == TraceExporterNone {
		return noop.NewTracerProvider(), func(context.Context) error { return nil }, nil
	}

	tracingResource, err := resource.New(
		ctx,
		resource.WithAttributes(attribute.String("service.name", config.ServiceName)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create OpenTelemetry resource: %w", err)
	}

	var processor sdktrace.SpanProcessor
	switch exporterName {
	case TraceExporterStdout:
		exporter, err := stdouttrace.New(stdouttrace.WithWriter(output))
		if err != nil {
			return nil, nil, fmt.Errorf("create stdout trace exporter: %w", err)
		}
		processor = sdktrace.NewSimpleSpanProcessor(exporter)
	case TraceExporterOTLPHTTP:
		exporter, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("create OTLP/HTTP trace exporter: %w", err)
		}
		processor = sdktrace.NewBatchSpanProcessor(exporter)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(tracingResource),
		sdktrace.WithSpanProcessor(processor),
	)
	return provider, provider.Shutdown, nil
}
