package com.p14n.postevent.catchup;

import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.broker.SystemEvent;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.data.UnprocessedEventFinder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service responsible for finding and submitting unprocessed events for
 * processing.
 * Integrates with {@link UnprocessedEventFinder} to locate events and triggers
 * catchup processing through the {@link SystemEventBroker}.
 *
 * <p>
 * The submitter periodically checks for unprocessed events and initiates
 * processing for their respective topics. It ensures that no events
 * are left unprocessed.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * DataSource dataSource = // initialize your datasource
 * SystemEventBroker broker = // initialize your broker
 * UnprocessedSubmitter submitter = new UnprocessedSubmitter(dataSource, broker);
 *
 * // Check for unprocessed events and submit them
 * submitter.submitUnprocessedEvents();
 * }</pre>
 */
public class UnprocessedSubmitter implements MessageSubscriber<SystemEvent>, OneAtATimeBehaviour {

    private final MessageBroker<Event, ?> targetBroker;
    private final DataSource ds;
    private final UnprocessedEventFinder unprocessedEventFinder;
    private final int batchSize;
    private final SystemEventBroker systemEventBroker;
    final AtomicInteger signals = new AtomicInteger(0);
    final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Creates a new UnprocessedSubmitter instance.
     *
     * @param systemEventBroker      Broker for publishing system events
     * @param ds                     Data source for database connections
     * @param unprocessedEventFinder Finder for unprocessed events
     * @param targetBroker           Broker for event processing
     * @param batchSize              Batch size for event processing
     */
    public UnprocessedSubmitter(SystemEventBroker systemEventBroker, DataSource ds,
            UnprocessedEventFinder unprocessedEventFinder,
            MessageBroker<Event, ?> targetBroker, int batchSize) {
        this.targetBroker = targetBroker;
        this.ds = ds;
        this.unprocessedEventFinder = unprocessedEventFinder;
        this.batchSize = batchSize;
        this.systemEventBroker = systemEventBroker;
    }

    /**
     * Finds and submits unprocessed events for catchup processing.
     * Queries the database for unprocessed events and triggers catchup
     * processing for each unique topic found.
     *
     * @return Number of topics submitted for processing
     * @throws SQLException if a database error occurs
     */
    private void resubmit() {
        try (Connection c = ds.getConnection()) {
            var events = unprocessedEventFinder.findUnprocessedEventsWithLimit(c, batchSize);
            for (var e : events) {
                targetBroker.publish(e.topic(), e);
            }
            if (events.size() == batchSize) {
                systemEventBroker.publish(SystemEvent.UnprocessedCheckRequired);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onMessage(SystemEvent message) {
        if (message == SystemEvent.UnprocessedCheckRequired) {
            oneAtATime(() -> resubmit(), () -> onMessage(message));
        }
    }

    @Override
    public AtomicInteger getSignals() {
        return signals;
    }

    @Override
    public AtomicBoolean getRunning() {
        return running;
    }
}
