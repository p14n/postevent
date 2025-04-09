package com.p14n.postevent.telemetry;

import java.util.function.Supplier;

import com.p14n.postevent.data.Traceable;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

public class OpenTelemetryFunctions {

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