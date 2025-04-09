package com.p14n.postevent;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.semconv.ResourceAttributes;

public class Opentelemetry {
    public static OpenTelemetry create(String serviceName) {
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .setResource(Resource.create(Attributes.of(
                        ResourceAttributes.SERVICE_NAME, serviceName)))
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(Resource.create(Attributes.of(
                        ResourceAttributes.SERVICE_NAME, serviceName)))
                .build();

        return OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();

    }
}
