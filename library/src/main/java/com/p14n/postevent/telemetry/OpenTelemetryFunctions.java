package com.p14n.postevent.telemetry;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.p14n.postevent.data.Traceable;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.api.OpenTelemetry;

/**
 * Utility class providing OpenTelemetry instrumentation functions for
 * distributed tracing.
 * Supports trace context propagation, span creation, and execution with
 * telemetry.
 *
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Trace context serialization and deserialization</li>
 * <li>Automated span management with error handling</li>
 * <li>Support for traceable events with metadata</li>
 * </ul>
 */
public class OpenTelemetryFunctions {

        /** Private constructor to prevent instantiation of utility class */
        private OpenTelemetryFunctions() {
        }

        /**
         * Serializes the current trace context into a string format.
         * Useful for passing trace context across process boundaries.
         *
         * @param ot OpenTelemetry instance to use for propagation
         * @return String representation of the trace context (traceparent)
         */
        public static String serializeTraceContext(OpenTelemetry ot) {
                Map<String, String> carrier = new HashMap<>();
                TextMapSetter<Map<String, String>> setter = Map::put;
                ot.getPropagators().getTextMapPropagator().inject(Context.current(), carrier, setter);
                return carrier.get("traceparent");
        }

        /**
         * Deserializes a trace context string back into an OpenTelemetry Context.
         *
         * @param ot          OpenTelemetry instance to use for propagation
         * @param traceparent Serialized trace context string
         * @return Reconstructed OpenTelemetry Context
         */
        public static Context deserializeTraceContext(OpenTelemetry ot, String traceparent) {
                Map<String, String> carrier = new HashMap<>();
                carrier.put("traceparent", traceparent);
                return ot.getPropagators().getTextMapPropagator().extract(Context.current(), carrier,
                                new MapTextMapGetter());
        }

        /**
         * Executes an action within a new trace span with detailed attributes.
         *
         * @param <T>         Return type of the action
         * @param ot          OpenTelemetry instance
         * @param tracer      Tracer to create spans
         * @param spanName    Name of the span to create
         * @param topic       Topic attribute for the span
         * @param eventId     Event ID attribute for the span
         * @param subject     Subject attribute for the span
         * @param traceparent Parent trace context (optional)
         * @param action      Action to execute within the span
         * @return Result of the action execution
         * @throws RuntimeException if the action throws an exception
         */
        public static <T> T processWithTelemetry(OpenTelemetry ot, Tracer tracer, String spanName, String topic,
                        String eventId,
                        String subject,
                        String traceparent,
                        Supplier<T> action) {
                Context parentContext = traceparent == null ? null
                                : OpenTelemetryFunctions.deserializeTraceContext(ot, traceparent);
                SpanBuilder sb = tracer.spanBuilder(spanName)
                                .setAttribute("topic", topic)
                                .setAttribute("event.id", eventId)
                                .setAttribute("subject", subject);
                if (parentContext != null) {
                        sb.setParent(parentContext);
                }
                Span span = sb.startSpan();
                try (Scope scope = span.makeCurrent()) {
                        return action.get();
                } catch (Exception e) {
                        span.recordException(e);
                        throw e;
                } finally {
                        span.end();
                }
        }

        /**
         * Executes an action within a new trace span with minimal configuration.
         *
         * @param <T>      Return type of the action
         * @param tracer   Tracer to create spans
         * @param spanName Name of the span to create
         * @param action   Action to execute within the span
         * @return Result of the action execution
         * @throws RuntimeException if the action throws an exception
         */
        public static <T> T processWithTelemetry(Tracer tracer, String spanName,
                        Supplier<T> action) {
                SpanBuilder sb = tracer.spanBuilder(spanName);
                Span span = sb.startSpan();
                try (Scope scope = span.makeCurrent()) {
                        return action.get();
                } catch (Exception e) {
                        span.recordException(e);
                        throw e;
                } finally {
                        span.end();
                }
        }

        /**
         * Executes an action within a new trace span using a Traceable event for
         * attributes.
         *
         * @param <T>      Return type of the action
         * @param ot       OpenTelemetry instance
         * @param tracer   Tracer to create spans
         * @param event    Traceable event containing span attributes
         * @param spanName Name of the span to create
         * @param action   Action to execute within the span
         * @return Result of the action execution
         * @throws RuntimeException if the action throws an exception
         */
        public static <T> T processWithTelemetry(OpenTelemetry ot, Tracer tracer, Traceable event, String spanName,
                        Supplier<T> action) {
                return processWithTelemetry(ot, tracer, spanName, event.topic(), event.id(), event.subject(),
                                event.traceparent(), action);
        }
}
