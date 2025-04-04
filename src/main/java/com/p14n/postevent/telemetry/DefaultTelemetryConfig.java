package com.p14n.postevent.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.semconv.ResourceAttributes;

public class DefaultTelemetryConfig implements TelemetryConfig {
    private static final String INSTRUMENTATION_NAME = "com.p14n.postevent";
    private final OpenTelemetry openTelemetry;
    private final Meter meter;
    private final Tracer tracer;

    public DefaultTelemetryConfig(String serviceName) {
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .setResource(Resource.create(Attributes.of(
                        ResourceAttributes.SERVICE_NAME, serviceName)))
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(Resource.create(Attributes.of(
                        ResourceAttributes.SERVICE_NAME, serviceName)))
                .build();

        openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();

        meter = openTelemetry.getMeter(INSTRUMENTATION_NAME);
        tracer = openTelemetry.getTracer(INSTRUMENTATION_NAME);
    }

    @Override
    public Meter getMeter() {
        return meter;
    }

    @Override
    public Tracer getTracer() {
        return tracer;
    }

    @Override
    public OpenTelemetry getOpenTelemetry() {
        return openTelemetry;
    }
}