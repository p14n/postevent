package com.p14n.postevent.data;

/**
 * Interface for objects that can be traced and identified in a distributed
 * system.
 * Provides essential methods for tracking and correlating events across
 * services.
 *
 * <p>
 * Key attributes:
 * </p>
 * <ul>
 * <li>{@code id}: Unique identifier for the traceable object</li>
 * <li>{@code topic}: Message routing or categorization identifier</li>
 * <li>{@code subject}: Business domain or entity identifier</li>
 * <li>{@code traceparent}: OpenTelemetry trace context identifier</li>
 * </ul>
 *
 * <p>
 * This interface is typically implemented by event and message classes to
 * support
 * distributed tracing and message correlation.
 * </p>
 */
public interface Traceable {

    /**
     * Returns the unique identifier of the traceable object.
     *
     * @return the unique identifier string
     */
    String id();

    /**
     * Returns the topic or stream identifier for message routing.
     *
     * @return the topic string
     */
    String topic();

    /**
     * Returns the business domain subject or entity identifier.
     *
     * @return the subject string
     */
    String subject();

    /**
     * Returns the OpenTelemetry trace parent identifier for distributed tracing.
     *
     * @return the trace parent string
     */
    String traceparent();
}
