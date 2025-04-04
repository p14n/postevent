package com.p14n.postevent.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;

public interface TelemetryConfig {
    Meter getMeter();

    Tracer getTracer();

    OpenTelemetry getOpenTelemetry();
}
