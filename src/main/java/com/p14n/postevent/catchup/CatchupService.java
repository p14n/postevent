package com.p14n.postevent.catchup;

import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.broker.SystemEvent;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.data.Event;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.p14n.postevent.db.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * Service to handle catching up on missed events for topics.
 */
public class CatchupService implements MessageSubscriber<SystemEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CatchupService.class);
    private static final int DEFAULT_BATCH_SIZE = 20;

    private final CatchupServerInterface catchupServer;
    private final DataSource datasource;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private final SystemEventBroker systemEventBroker;

    public CatchupService(DataSource ds, CatchupServerInterface catchupServer, SystemEventBroker systemEventBroker) {
        this.datasource = ds;
        this.catchupServer = catchupServer;
        this.systemEventBroker = systemEventBroker;
    }

    /**
     * Catches up a topic by processing events since their last high water
     * mark.
     * 
     * @param topicName The name of the topic
     * @return The number of events processed
     * @throws SQLException If a database error occurs
     */
    public int catchup(String topicName) {

        try (Connection conn = datasource.getConnection()) {
            conn.setAutoCommit(false);

            long currentHwm = getCurrentHwm(conn, topicName);

            // Find the next gap end (lowest idn greater than hwm)
            long gapEnd = findGapEnd(conn, currentHwm);

            LOGGER.info(String.format("Current HWM %d highest message in gap %d",
                    currentHwm, gapEnd));

            if (gapEnd <= currentHwm) {
                LOGGER.info("No new events to process for topic: " + topicName);
                return 0;
            }

            // Fetch events from catchup server
            List<Event> events = catchupServer.fetchEvents(currentHwm, gapEnd, batchSize, topicName);

            if (events.isEmpty()) {
                LOGGER.info("No events found in range for topic: " + topicName);
                return 0;
            }

            // Write events to messages table
            int processedCount = writeEventsToMessagesTable(conn, events);

            // Update the high water mark
            long newHwm = events.get(events.size() - 1).idn();
            updateHwm(conn, topicName, currentHwm, newHwm);

            LOGGER.info(String.format("Processed %d events for topic %s, updated HWM from %d to %d",
                    processedCount, topicName, currentHwm, newHwm));

            conn.commit();
            systemEventBroker.publish(SystemEvent.UnprocessedCheckRequired);
            return processedCount;
        } catch (SQLException e) {
            LOGGER.error("Failed to catch up", e);
            return 0;
        }
    }

    private long getCurrentHwm(Connection connection, String topicName) throws SQLException {
        String sql = "SELECT hwm FROM postevent.contiguous_hwm WHERE topic_name = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topicName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("hwm");
                } else {
                    // If no record exists, initialize with 0 and return 0
                    initializeHwm(connection, topicName);
                    return 0;
                }
            }
        }
    }

    private void initializeHwm(Connection connection, String topicName) throws SQLException {
        String sql = "INSERT INTO postevent.contiguous_hwm (topic_name, hwm) VALUES (?, 0) ON CONFLICT DO NOTHING";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topicName);
            stmt.executeUpdate();
        }
        getCurrentHwm(connection, topicName);
    }

    private long findGapEnd(Connection connection, long currentHwm) throws SQLException {
        String sql = "SELECT MIN(idn) as next_idn FROM postevent.messages WHERE idn > ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, currentHwm);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next() && rs.getObject("next_idn") != null) {
                    return rs.getLong("next_idn");
                } else {
                    // If no higher idn exists, return the current hwm
                    return currentHwm;
                }
            }
        }
    }

    private int writeEventsToMessagesTable(Connection connection, List<Event> events) throws SQLException {
        int count = 0;
        String sql = "INSERT INTO postevent.messages (" +
                SQL.EXT_COLS +
                ") VALUES (" + SQL.EXT_PH +
                ") ON CONFLICT DO NOTHING";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (Event event : events) {
                SQL.setEventOnStatement(stmt, event);
                SQL.setTimeIDNAndTopic(stmt, event);
                count += stmt.executeUpdate();
            }
        }

        return count;
    }

    private void updateHwm(Connection connection, String topicName, long currentHwm, long newHwm)
            throws SQLException {
        String sql = "UPDATE postevent.contiguous_hwm SET hwm = ? WHERE topic_name = ? AND hwm = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, newHwm);
            stmt.setString(2, topicName);
            stmt.setLong(3, currentHwm);

            int updated = stmt.executeUpdate();

            if (updated == 0) {
                LOGGER.warn("Failed to update HWM for topic " + topicName +
                        ". Current HWM may have changed.");
                throw new SQLException("Optimistic locking failure: HWM has been modified by another process");
            }
        }
    }

    private AtomicInteger signals = new AtomicInteger(0);
    private AtomicBoolean running = new AtomicBoolean(false);

    @Override
    public void onMessage(SystemEvent message) {
        if (Objects.requireNonNull(message) == SystemEvent.CatchupRequired) {
            signals.incrementAndGet();
            if (running.get()) {
                return;
            }
            synchronized (running) {
                if (!running.get()) {
                    running.set(true);
                    signals.set(0);
                    catchup(message.topic);
                    running.set(false);
                    if (signals.get() > 0) {
                        onMessage(message);
                    }
                }
            }
        }
    }

    private static class GapCheckResult {
        final boolean gapFound;
        final long lastContiguousIdn;

        GapCheckResult(boolean gapFound, long lastContiguousIdn) {
            this.gapFound = gapFound;
            this.lastContiguousIdn = lastContiguousIdn;
        }
    }

    private GapCheckResult processMessages(ResultSet rs, long currentHwm) throws SQLException {
        long expectedNext = currentHwm + 1;
        long lastContiguousIdn = currentHwm;
        while (rs.next()) {
            long actualIdn = rs.getLong("idn");
            if (actualIdn > expectedNext) {
                LOGGER.info("Gap found: Expected {0}, found {1} (gap of {2})",
                        new Object[] { expectedNext, actualIdn, actualIdn - expectedNext });
                return new GapCheckResult(true, lastContiguousIdn);
            }
            lastContiguousIdn = actualIdn;
            expectedNext = actualIdn + 1;
        }
        return new GapCheckResult(false, lastContiguousIdn);
    }

    /**
     * Checks for gaps in the message sequence and updates the HWM to the last
     * contiguous message.
     * 
     * @param topicName  The name of the topic
     * @param currentHwm The current high water mark to start checking from
     * @return true if a gap was found, false if no gaps were found
     * @throws SQLException If a database error occurs
     */
    public boolean hasSequenceGap(String topicName, long currentHwm) throws SQLException {
        LOGGER.debug("Checking for sequence gaps after HWM {0} for topic {1}",
                new Object[] { currentHwm, topicName });
        String sql = "SELECT idn FROM postevent.messages WHERE idn > ? ORDER BY idn";

        try (Connection connection = datasource.getConnection();
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);

            stmt.setLong(1, currentHwm);
            try (ResultSet rs = stmt.executeQuery()) {
                GapCheckResult result = processMessages(rs, currentHwm);
                if (result.lastContiguousIdn > currentHwm) {
                    LOGGER.debug("Updating HWM from {0} to {1} for topic {2}",
                            new Object[] { currentHwm, result.lastContiguousIdn, topicName });
                    updateHwm(connection, topicName, currentHwm, result.lastContiguousIdn);
                }
                if (!result.gapFound) {
                    LOGGER.debug("No sequence gaps found after HWM for topic {0}",
                            new Object[] { topicName });
                }
                connection.commit();
                return result.gapFound;
            }
        }
    }
}