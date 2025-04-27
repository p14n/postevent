package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.data.Traceable;

import java.sql.Connection;

/**
 * A record that wraps an {@link Event} with its associated database
 * {@link Connection},
 * enabling transactional processing of events. This class implements
 * {@link Traceable}
 * to maintain tracing context through the event processing pipeline.
 *
 * <p>
 * The TransactionalEvent serves as a container that:
 * <ul>
 * <li>Maintains the database connection context for transactional
 * operations</li>
 * <li>Preserves the original event data and metadata</li>
 * <li>Delegates tracing information to the underlying event</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * Connection conn = dataSource.getConnection();
 * Event event = Event.create(...);
 * TransactionalEvent txEvent = new TransactionalEvent(conn, event);
 *
 * // Use the transactional event
 * processEventInTransaction(txEvent);
 * }</pre>
 *
 * @param connection The database connection for transactional operations
 * @param event      The underlying event being processed
 */
public record TransactionalEvent(Connection connection, Event event) implements Traceable {

    /**
     * Returns the unique identifier of the underlying event.
     *
     * @return the event's unique identifier
     */
    @Override
    public String id() {
        return event.id();
    }

    /**
     * Returns the topic of the underlying event.
     *
     * @return the event's topic
     */
    @Override
    public String topic() {
        return event.topic();
    }

    /**
     * Returns the subject of the underlying event.
     *
     * @return the event's subject
     */
    @Override
    public String subject() {
        return event.subject();
    }

    /**
     * Returns the OpenTelemetry trace parent identifier of the underlying event.
     *
     * @return the event's trace parent identifier
     */
    @Override
    public String traceparent() {
        return event.traceparent();
    }
}
