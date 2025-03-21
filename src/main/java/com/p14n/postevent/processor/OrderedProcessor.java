package com.p14n.postevent.processor;

import com.p14n.postevent.data.Event;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processes events in order, ensuring that all events with the same subject
 * and lower idn values are processed first.
 */
public class OrderedProcessor {
    private static final Logger LOGGER = Logger.getLogger(OrderedProcessor.class.getName());
    private final BiFunction<Connection, Event, Boolean> processorFunction;

    /**
     * Creates a new OrderedProcessor with the specified processing function.
     *
     * @param processorFunction Function that processes an event with a database
     *                          connection
     *                          and returns true if processing was successful
     */
    public OrderedProcessor(BiFunction<Connection, Event, Boolean> processorFunction) {
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
                LOGGER.info("Skipping event " + event.id() + " as there are unprocessed prior events");
                return false;
            }

            // Try to update the status to 'p' (processed)
            if (!updateEventStatus(connection, event)) {
                LOGGER.info("Skipping event " + event.id() + " as it's already being processed");
                return false;
            }

            // Process the event and handle the result
            boolean success = processEventWithFunction(connection, event);

            if (success) {
                connection.commit();
                LOGGER.info("Successfully processed event " + event.id());
                return true;
            } else {
                return false; // Already rolled back in processEventWithFunction
            }
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Database error while processing event " + event.id(), e);
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
                "WHERE subject = ? AND idn < ? AND status != 'p'";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, event.subject());
            stmt.setLong(2, event.idn());

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
                "WHERE idn = ? AND status = 'u'";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, event.idn());

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
                LOGGER.info("Processing failed for event " + event.id());
                performRollback(connection);
            }
            return success;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error processing event " + event.id(), e);
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
            LOGGER.log(Level.SEVERE, "Error rolling back transaction", rollbackEx);
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
            LOGGER.log(Level.SEVERE, "Error restoring auto-commit state", e);
        }
    }
}