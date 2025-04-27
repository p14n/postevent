package com.p14n.postevent.processor;

import com.p14n.postevent.broker.SystemEvent;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.data.Event;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes events in order, ensuring that all events with the same subject
 * and lower idn values are processed first.
 */
public class OrderedProcessor {

    private static final Logger logger = LoggerFactory.getLogger(OrderedProcessor.class);

    private final BiFunction<Connection, Event, Boolean> processorFunction;
    private final SystemEventBroker systemEventBroker;;

    /**
     * Creates a new OrderedProcessor with the specified processing function.
     *
     * @param systemEventBroker The broker for system events
     * @param processorFunction Function that processes an event with a database
     *                          connection
     *                          and returns true if processing was successful
     */
    public OrderedProcessor(SystemEventBroker systemEventBroker,
            BiFunction<Connection, Event, Boolean> processorFunction) {
        this.systemEventBroker = systemEventBroker;
        this.processorFunction = processorFunction;
    }

    /**
     * Processes the event if all previous events with the same subject have been
     * processed.
     *
     * @param connection Database connection to use
     * @param event      Event to process
     * @return true if the event was processed, false otherwise
     */
    public boolean process(Connection connection, Event event) {
        try {
            // Set auto-commit to false for our transaction
            connection.setAutoCommit(false);

            // Check if there are any unprocessed events with the same subject and lower idn
            if (hasUnprocessedPriorEvents(connection, event)) {
                logger.atDebug().log(() -> "Skipping event " + event.id() + " (idn: " + event.idn() + ")"
                        + " as there are unprocessed prior events");
                return false;
            }

            // Try to update the status to 'p' (processed)
            if (!updateEventStatus(connection, event)) {
                logger.atDebug().log(() -> "Skipping event " + event.id() + " (idn: " + event.idn() + ")"
                        + " as it's already being processed");
                return false;
            }
            if (!previousEventExists(connection, event)) {
                logger.atDebug().log(() -> "Skipping event " + event.id() + " (idn: " + event.idn() + ") topic "
                        + event.topic() + " as the previous event has not reached the client");
                systemEventBroker.publish(SystemEvent.CatchupRequired.withTopic(event.topic()));
                return false;
            }

            // Process the event and handle the result
            boolean success = processEventWithFunction(connection, event);

            if (success) {
                connection.commit();
                logger.atDebug()
                        .log(() -> "Successfully processed event " + event.id() + " (idn: " + event.idn() + ") "
                                + event.topic());
                return true;
            } else {
                return false; // Already rolled back in processEventWithFunction
            }
        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error processing event");
            performRollback(connection);
            return false;
        }
    }

    /**
     * Checks if there are any unprocessed events with the same subject and lower
     * idn.
     *
     * @param connection Database connection
     * @param event      Current event
     * @return true if there are unprocessed prior events, false otherwise
     * @throws SQLException if a database error occurs
     */
    private boolean hasUnprocessedPriorEvents(Connection connection, Event event) throws SQLException {
        String sql = "SELECT COUNT(*) FROM postevent.messages " +
                "WHERE subject = ? AND topic = ? AND idn < ? AND status != 'p'";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, event.subject());
            stmt.setString(2, event.topic());
            stmt.setLong(3, event.idn());

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    return count > 0;
                }
                return false;
            }
        }
    }

    /**
     * Checks that the previous idn has reached the client
     *
     * @param connection Database connection
     * @param event      Current event
     * @return true if the previous event exists, false otherwise
     * @throws SQLException if a database error occurs
     */
    private boolean previousEventExists(Connection connection, Event event) throws SQLException {
        String sql = "SELECT hwm FROM postevent.contiguous_hwm WHERE topic_name = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, event.topic());

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long hwm = rs.getLong(1);
                    return hwm >= event.idn() - 1;
                }
                return false;
            }
        }
    }

    /**
     * Updates the event status to 'p' (processed) if it's currently 'u'
     * (unprocessed).
     *
     * @param connection Database connection
     * @param event      Event to update
     * @return true if the update was successful, false otherwise
     * @throws SQLException if a database error occurs
     */
    private boolean updateEventStatus(Connection connection, Event event) throws SQLException {
        String sql = "UPDATE postevent.messages SET status = 'p' " +
                "WHERE idn = ? AND topic = ? AND status = 'u'";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, event.idn());
            stmt.setString(2, event.topic());

            int updatedRows = stmt.executeUpdate();
            return updatedRows > 0;
        }
    }

    /**
     * Processes the event using the processor function and handles exceptions.
     *
     * @param connection Database connection
     * @param event      Event to process
     * @return true if processing was successful, false otherwise
     * @throws SQLException if a database error occurs during rollback
     */
    private boolean processEventWithFunction(Connection connection, Event event) throws SQLException {
        try {
            boolean success = processorFunction.apply(connection, event);
            if (!success) {
                logger.atError().log("Processing failed for event " + event.id());
                performRollback(connection);
            }
            return success;
        } catch (Exception e) {
            logger.atError().setCause(e).log("Error processing event " + event.id());
            performRollback(connection);
            return false;
        }
    }

    /**
     * Performs a rollback on the connection and logs any errors.
     *
     * @param connection Database connection
     */
    private void performRollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException rollbackEx) {
            logger.atError().setCause(rollbackEx).log("Error rolling back transaction");
        }
    }

    /**
     * Restores the auto-commit state of the connection.
     *
     * @param connection Database connection
     * @param autoCommit Original auto-commit state
     */
    private void restoreAutoCommit(Connection connection, boolean autoCommit) {
        try {
            connection.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error restoring auto-commit state");
        }
    }
}