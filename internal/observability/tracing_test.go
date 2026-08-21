package observability

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/propagation"
)

func TestStdoutTraceExporterWritesCompletedSpan(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	provider, shutdown, err := OpenTracerProvider(context.Background(), TraceConfig{
		Exporter:    TraceExporterStdout,
		ServiceName: "forgeflow-test",
	}, &output)
	if err != nil {
		t.Fatalf("OpenTracerProvider() error = %v", err)
	}
	_, span := provider.Tracer("forgeflow/test").Start(context.Background(), "observed-test-span")
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("trace shutdown error = %v", err)
	}
	if !strings.Contains(output.String(), "observed-test-span") || !strings.Contains(output.String(), "forgeflow-test") {
		t.Fatalf("stdout trace output = %q", output.String())
	}
}

func TestNoopTracingStillPropagatesIncomingW3CContext(t *testing.T) {
	t.Parallel()

	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	instrumentation := NewNoopInstrumentation()
	ctx := instrumentation.Propagator().Extract(
		context.Background(),
		propagation.MapCarrier{"traceparent": traceParent},
	)
	ctx, span := instrumentation.Tracer("forgeflow/test").Start(ctx, "noop-child")
	defer span.End()
	carrier := propagation.MapCarrier{}
	instrumentation.Propagator().Inject(ctx, carrier)
	if got := carrier.Get("traceparent"); got != traceParent {
		t.Fatalf("propagated traceparent = %q, want %q", got, traceParent)
	}
}

func TestTraceConfigRejectsUnsupportedExporter(t *testing.T) {
	t.Parallel()

	err := (TraceConfig{Exporter: "zipkin", ServiceName: "forgeflow"}).Validate()
	if err == nil {
		t.Fatal("TraceConfig.Validate() error = nil")
	}
}
