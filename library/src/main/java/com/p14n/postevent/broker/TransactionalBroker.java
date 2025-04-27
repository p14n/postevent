package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.processor.OrderedProcessor;

import io.opentelemetry.api.OpenTelemetry;

import javax.sql.DataSource;

import static com.p14n.postevent.telemetry.OpenTelemetryFunctions.processWithTelemetry;

import java.sql.Connection;

/**
 * A specialized message broker that provides transactional delivery of events
 * to subscribers.
 * Extends {@link DefaultMessageBroker} to ensure events are processed within
 * database transactions.
 *
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Database transaction support for each message delivery</li>
 * <li>Ordered processing of events using {@link OrderedProcessor}</li>
 * <li>Integration with OpenTelemetry for transaction monitoring</li>
 * <li>Error handling with subscriber notification</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * TransactionalBroker broker = new TransactionalBroker(dataSource, openTelemetry, systemEventBroker);
 * broker.subscribe("orders", new TransactionalEventHandler());
 * broker.publish("orders", event);
 * }</pre>
 */
public class TransactionalBroker extends DefaultMessageBroker<Event, TransactionalEvent> {
    private final DataSource ds;
    private final SystemEventBroker systemEventBroker;

    /**
     * Creates a new transactional broker with custom executor configuration.
     *
     * @param ds                the data source for database connections
     * @param asyncExecutor     the executor for handling asynchronous message
     *                          delivery
     * @param ot                the OpenTelemetry instance for metrics and tracing
     * @param systemEventBroker the broker for system events
     */
    public TransactionalBroker(DataSource ds, AsyncExecutor asyncExecutor, OpenTelemetry ot,
            SystemEventBroker systemEventBroker) {
        super(asyncExecutor, ot, "transactional_broker");
        this.ds = ds;
        this.systemEventBroker = systemEventBroker;
    }

    /**
     * Creates a new transactional broker with default executor configuration.
     *
     * @param ds                the data source for database connections
     * @param ot                the OpenTelemetry instance for metrics and tracing
     * @param systemEventBroker the broker for system events
     */
    public TransactionalBroker(DataSource ds, OpenTelemetry ot, SystemEventBroker systemEventBroker) {
        super(ot, "transactional_broker");
        this.ds = ds;
        this.systemEventBroker = systemEventBroker;
    }

    /**
     * Publishes a message to subscribers within a database transaction.
     * Each subscriber receives the message in its own transaction context.
     *
     * <p>
     * The publishing process:
     * </p>
     * <ol>
     * <li>Validates the message and topic</li>
     * <li>Creates a new database connection for each subscriber</li>
     * <li>Processes the message using {@link OrderedProcessor}</li>
     * <li>Handles delivery within a transaction context</li>
     * <li>Records metrics for successful delivery</li>
     * </ol>
     *
     * @param topic   the topic to publish to
     * @param message the event message to publish
     * @throws IllegalStateException    if the broker is closed
     * @throws IllegalArgumentException if topic or message is null
     */
    @Override
    public void publish(String topic, Event message) {
        if (!canProcess(topic, message)) {
            return;
        }

        metrics.recordPublished(topic);

        // Deliver to all subscribers
        for (MessageSubscriber<TransactionalEvent> subscriber : topicSubscribers.get(topic)) {
            try (Connection c = ds.getConnection()) {
                processWithTelemetry(openTelemetry, tracer, message, "ordered_process", () -> {
                    var op = new OrderedProcessor(systemEventBroker, (connection, event) -> {
                        return processWithTelemetry(openTelemetry, tracer, message, "message_transaction", () -> {
                            try {
                                subscriber.onMessage(new TransactionalEvent(connection, event));
                                metrics.recordReceived(topic);
                                return true;
                            } catch (Exception e) {
                                try {
                                    subscriber.onError(e);
                                } catch (Exception ignored) {
                                }
                                return false;
                            }
                        });
                    });
                    op.process(c, message);
                    return null;
                });
            } catch (Exception e) {
                try {
                    subscriber.onError(e);
                } catch (Exception ignored) {
                    // If error handling fails, we ignore it to protect other subscribers
                }
            }
        }
    }

    @Override
    public TransactionalEvent convert(Event m) {
        return null;
    }
}
