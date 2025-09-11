package com.p14n.postevent.broker;

import io.opentelemetry.api.OpenTelemetry;

/**
 * Specialized message broker for handling system-level events.
 * Extends {@link DefaultMessageBroker} to provide a dedicated channel for
 * internal system coordination events.
 *
 * <p>
 * This broker handles {@link SystemEvent} messages for internal coordination
 * such as:
 * <ul>
 * <li>Catchup processing signals</li>
 * <li>Unprocessed event checks</li>
 * <li>Latest event fetch requests</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * SystemEventBroker broker = new SystemEventBroker(openTelemetry);
 * broker.subscribe(subscriber);
 * broker.publish(SystemEvent.CatchupRequired.withTopic("orders"));
 * }</pre>
 */
public class SystemEventBroker extends DefaultMessageBroker<SystemEvent, SystemEvent> {

    /**
     * Creates a new system event broker with custom executor configuration.
     *
     * @param asyncExecutor the executor for handling asynchronous message delivery
     * @param ot            the OpenTelemetry instance for metrics and tracing
     */
    public SystemEventBroker(AsyncExecutor asyncExecutor, OpenTelemetry ot) {
        super(asyncExecutor, ot, "system_events");
    }

    /**
     * Creates a new system event broker with default executor configuration.
     *
     * @param ot the OpenTelemetry instance for metrics and tracing
     */
    public SystemEventBroker(OpenTelemetry ot) {
        super(ot, "system_events");
    }

    @Override
    public SystemEvent convert(SystemEvent m) {
        return m;
    }

    /**
     * Publishes a system event to the default system topic.
     *
     * @param event the system event to publish
     */
    public void publish(SystemEvent event) {
        publish("system", event);
    }

    /**
     * Subscribes to system events on the default system topic.
     *
     * @param subscriber the subscriber to receive system events
     */
    public void subscribe(MessageSubscriber<SystemEvent> subscriber) {
        subscribe("system", subscriber);
    }
}
