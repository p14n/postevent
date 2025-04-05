package com.p14n.postevent.telemetry;

import java.util.function.Supplier;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.data.Traceable;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.semconv.ResourceAttributes;

public class OpenTelemetryFunctions {
        private static final String INSTRUMENTATION_NAME = "com.p14n.postevent";

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

        public static <T> T processWithTelemetry(Tracer tracer, String spanName, String topic, String eventId,
                        String subject,
                        Supplier<T> action) {
                Span span = tracer.spanBuilder(spanName)
                                .setAttribute("topic", topic)
                                .setAttribute("event.id", eventId)
                                .setAttribute("subject", subject)
                                .startSpan();
                try (Scope scope = span.makeCurrent()) {
                        return action.get();
                } catch (Exception e) {
                        span.recordException(e);
                        throw e;
                } finally {
                        span.end();
                }
        }

        public static <T> T processWithTelemetry(Tracer tracer, Traceable event, String spanName,
                        Supplier<T> action) {
                return processWithTelemetry(tracer, spanName, event.topic(), event.id(), event.subject(), action);
        }

}