package com.p14n.postevent.catchup;

import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.SystemEvent;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.db.SQL;

import javax.sql.DataSource;
import java.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A message broker implementation that provides persistence for events before
 * forwarding them to a target broker.
 * This broker ensures reliable event delivery by first persisting events to a
 * database and then forwarding them
 * to subscribers only after successful persistence.
 *
 * <p>
 * Key features:
 * <ul>
 * <li>Event persistence before delivery</li>
 * <li>High-water mark (HWM) tracking for each topic</li>
 * <li>Automatic catchup triggering for missed events</li>
 * <li>Transaction management for reliable persistence</li>
 * <li>Integration with system event notifications</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * DataSource dataSource = // initialize your datasource
 * MessageBroker<Event, OutT> targetBroker = // initialize target broker
 * SystemEventBroker systemEventBroker = // initialize system event broker
 * 
 * PersistentBroker<OutT> broker = new PersistentBroker<>(
 *     targetBroker, 
 *     dataSource, 
 *     systemEventBroker
 * );
 * 
 * // Use the broker
 * broker.publish("orders", event);
 * }</pre>
 *
 * @param <OutT> The type of messages that the target broker handles
 */
public class PersistentBroker<OutT> implements MessageBroker<Event, OutT>, AutoCloseable, MessageSubscriber<Event> {
    private static final Logger logger = LoggerFactory.getLogger(PersistentBroker.class);
    private static final String INSERT_SQL = "INSERT INTO postevent.messages (" + SQL.EXT_COLS +
            ") VALUES (" + SQL.EXT_PH + ") ON CONFLICT DO NOTHING";
    private static final String UPDATE_HWM_SQL = "UPDATE postevent.contiguous_hwm set hwm=? where topic_name=? and hwm=?";

    private final MessageBroker<Event, OutT> targetBroker;
    private final DataSource dataSource;
    private final SystemEventBroker systemEventBroker;

    /**
     * Creates a new PersistentBroker instance.
     *
     * @param targetBroker      The broker to forward events to after persistence
     * @param dataSource        The data source for event persistence
     * @param systemEventBroker The broker for system event notifications
     */
    public PersistentBroker(MessageBroker<Event, OutT> targetBroker,
            DataSource dataSource,
            SystemEventBroker systemEventBroker) {
        this.targetBroker = targetBroker;
        this.dataSource = dataSource;
        this.systemEventBroker = systemEventBroker;
    }

    /**
     * Publishes an event to the specified topic with persistence.
     * The event is first persisted to the database, and only after successful
     * persistence
     * is it forwarded to the target broker. If the high-water mark update fails,
     * a catchup event is triggered.
     *
     * @param topic The topic to publish to
     * @param event The event to publish
     * @throws RuntimeException if persistence or forwarding fails
     */
    @Override
    public void publish(String topic, Event event) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
                SQL.setEventOnStatement(stmt, event);
                SQL.setTimeIDNAndTopic(stmt, event);
                stmt.executeUpdate();
            }
            int updates = 0;
            try (PreparedStatement stmt = conn.prepareStatement(UPDATE_HWM_SQL)) {
                stmt.setLong(1, event.idn());
                stmt.setString(2, event.topic());
                stmt.setLong(3, event.idn() - 1);
                updates = stmt.executeUpdate();
                if (updates < 1) {
                    logger.atDebug().log("Publishing catchup required event for topic " + topic);
                    systemEventBroker
                            .publish(SystemEvent.CatchupRequired.withTopic(event.topic()));

                }
            }

            conn.commit();

            // Forward to actual subscriber after successful persistence
            if (updates > 0) {
                logger.atDebug().log("Forwarding event to target broker for topic " + topic);
                targetBroker.publish(topic, event);
            }

        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error persisting and forwarding event");
            SQL.handleSQLException(e, conn);
            throw new RuntimeException("Failed to persist and forward event", e);
        } finally {
            SQL.closeConnection(conn);
        }
    }

    /**
     * Subscribes to events on the specified topic.
     *
     * @param topic      The topic to subscribe to
     * @param subscriber The subscriber that will receive events
     * @return true if subscription was successful, false otherwise
     */
    @Override
    public boolean subscribe(String topic, MessageSubscriber<OutT> subscriber) {
        return targetBroker.subscribe(topic, subscriber);
    }

    /**
     * Unsubscribes from events on the specified topic.
     *
     * @param topic      The topic to unsubscribe from
     * @param subscriber The subscriber to remove
     * @return true if unsubscription was successful, false otherwise
     */
    @Override
    public boolean unsubscribe(String topic, MessageSubscriber<OutT> subscriber) {
        return targetBroker.unsubscribe(topic, subscriber);
    }

    /**
     * Closes the broker and its resources.
     */
    @Override
    public void close() {
        targetBroker.close();
    }

    /**
     * Converts an event message.
     * This implementation returns null as conversion is handled by the target
     * broker.
     *
     * @param m The event to convert
     * @return null
     */
    @Override
    public OutT convert(Event m) {
        return null;
    }

    /**
     * Handles incoming messages by publishing them to the appropriate topic.
     *
     * @param message The event message to handle
     */
    @Override
    public void onMessage(Event message) {
        logger.atDebug().log("Received event for persistence");
        publish(message.topic(), message);
    }
}
