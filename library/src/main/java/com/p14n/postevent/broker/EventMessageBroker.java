package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;
import io.opentelemetry.api.OpenTelemetry;

/**
 * A specialized message broker implementation for handling {@link Event}
 * messages.
 * Extends {@link DefaultMessageBroker} to provide direct pass-through of events
 * without any transformation.
 *
 * <p>
 * This broker is designed for simple event distribution scenarios where the
 * input
 * and output event types are identical. It maintains all the thread-safety and
 * telemetry capabilities of the parent class while simplifying the event
 * handling
 * process.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * EventMessageBroker broker = new EventMessageBroker(openTelemetry, "events");
 * broker.subscribe("orders", event -> {
 *     // Handle the event directly
 * });
 * broker.publish("orders", new Event(...));
 * }</pre>
 */
public class EventMessageBroker extends DefaultMessageBroker<Event, Event> {

    /**
     * Creates a new EventMessageBroker with custom executor configuration.
     *
     * @param asyncExecutor The executor for handling asynchronous message delivery
     * @param ot            The OpenTelemetry instance for metrics and tracing
     * @param scopeName     The scope name for OpenTelemetry instrumentation
     */
    public EventMessageBroker(AsyncExecutor asyncExecutor, OpenTelemetry ot, String scopeName) {
        super(asyncExecutor, ot, scopeName);
    }

    /**
     * Creates a new EventMessageBroker with default executor configuration.
     *
     * @param ot        The OpenTelemetry instance for metrics and tracing
     * @param scopeName The scope name for OpenTelemetry instrumentation
     */
    public EventMessageBroker(OpenTelemetry ot, String scopeName) {
        super(ot, scopeName);
    }

    /**
     * Implements the conversion method from the parent class.
     * Simply returns the input event as-is since no transformation is needed.
     *
     * @param m The event to convert
     * @return The same event instance
     */
    @Override
    public Event convert(Event m) {
        return m;
    }
}
