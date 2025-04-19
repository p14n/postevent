package com.p14n.postevent.telemetry;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.p14n.postevent.data.Traceable;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.api.OpenTelemetry;

public class OpenTelemetryFunctions {
        public static String serializeTraceContext(OpenTelemetry ot) {
                Map<String, String> carrier = new HashMap<>();
                TextMapSetter<Map<String, String>> setter = Map::put;
                ot.getPropagators().getTextMapPropagator().inject(Context.current(), carrier, setter);
                return carrier.get("traceparent"); // Serialized trace context
        }
        public static Context deserializeTraceContext(OpenTelemetry ot,String traceparent) {
                Map<String, String> carrier = new HashMap<>();
                carrier.put("traceparent", traceparent);
                return ot.getPropagators().getTextMapPropagator().extract(Context.current(), carrier, new MapTextMapGetter());
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