package com.p14n.postevent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service to handle catching up on missed events for subscribers.
 */
public class CatchupService {
    private static final Logger LOGGER = Logger.getLogger(CatchupService.class.getName());
    private static final int DEFAULT_BATCH_SIZE = 20;

    private final Connection connection;
    private final CatchupServer catchupServer;
    private final String topic;

    public CatchupService(Connection connection, CatchupServer catchupServer, String topic) {
        this.connection = connection;
        this.catchupServer = catchupServer;
        this.topic = topic;
    }

    /**
     * Catches up a subscriber by processing events since their last high water
     * mark.
     * 
     * @param subscriberName The name of the subscriber
     * @param batchSize      The maximum number of events to process in one batch
     * @return The number of events processed
     * @throws SQLException If a database error occurs
     */
    public int catchup(String subscriberName, int batchSize) throws SQLException {
        // Find current high water mark for the subscriber
        long currentHwm = getCurrentHwm(subscriberName);

        // Find the next gap end (lowest idn greater than hwm)
        long gapEnd = findGapEnd(currentHwm);

        LOGGER.info(String.format("Current HWM %d highest message in gap %d",
                currentHwm, gapEnd));

        if (gapEnd <= currentHwm) {
            LOGGER.info("No new events to process for subscriber: " + subscriberName);
            return 0;
        }

        // Fetch events from catchup server
        List<Event> events = catchupServer.fetchEvents(currentHwm, gapEnd, DEFAULT_BATCH_SIZE);

        if (events.isEmpty()) {
            LOGGER.info("No events found in range for subscriber: " + subscriberName);
            return 0;
        }

        // Write events to messages table
        int processedCount = writeEventsToMessagesTable(events);

        // Update the high water mark
        long newHwm = events.get(events.size() - 1).idn();
        updateHwm(subscriberName, currentHwm, newHwm);

        LOGGER.info(String.format("Processed %d events for subscriber %s, updated HWM from %d to %d",
                processedCount, subscriberName, currentHwm, newHwm));

        return processedCount;
    }

    private long getCurrentHwm(String subscriberName) throws SQLException {
        String sql = "SELECT hwm FROM postevent.contiguous_hwm WHERE subscriber_name = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, subscriberName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("hwm");
                } else {
                    // If no record exists, initialize with 0 and return 0
                    initializeHwm(subscriberName);
                    return 0;
                }
            }
        }
    }

    private void initializeHwm(String subscriberName) throws SQLException {
        String sql = "INSERT INTO postevent.contiguous_hwm (subscriber_name, hwm) VALUES (?, 0)";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, subscriberName);
            stmt.executeUpdate();
        }
    }

    private long findGapEnd(long currentHwm) throws SQLException {
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

    private int writeEventsToMessagesTable(List<Event> events) throws SQLException {
        int count = 0;
        String sql = """
                INSERT INTO postevent.messages
                (id, source, datacontenttype, dataschema, subject, data, idn)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (Event event : events) {
                stmt.setString(1, event.id());
                stmt.setString(2, event.source());
                stmt.setString(3, event.datacontenttype());
                stmt.setString(4, event.dataschema());
                stmt.setString(5, event.subject());
                stmt.setBytes(6, event.data());
                stmt.setLong(7, event.idn());

                count += stmt.executeUpdate();
            }
        }

        return count;
    }

    private void updateHwm(String subscriberName, long currentHwm, long newHwm) throws SQLException {
        String sql = "UPDATE postevent.contiguous_hwm SET hwm = ? WHERE subscriber_name = ? AND hwm = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, newHwm);
            stmt.setString(2, subscriberName);
            stmt.setLong(3, currentHwm);

            int updated = stmt.executeUpdate();

            if (updated == 0) {
                LOGGER.warning("Failed to update HWM for subscriber " + subscriberName +
                        ". Current HWM may have changed.");
                throw new SQLException("Optimistic locking failure: HWM has been modified by another process");
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
                LOGGER.log(Level.INFO, "Gap found: Expected {0}, found {1} (gap of {2})",
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
     * @param subscriberName The name of the subscriber
     * @param currentHwm     The current high water mark to start checking from
     * @return true if a gap was found, false if no gaps were found
     * @throws SQLException If a database error occurs
     */
    public boolean hasSequenceGap(String subscriberName, long currentHwm) throws SQLException {
        LOGGER.log(Level.FINE, "Checking for sequence gaps after HWM {0} for subscriber {1}",
                new Object[] { currentHwm, subscriberName });
        String sql = "SELECT idn FROM postevent.messages WHERE idn > ? ORDER BY idn";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, currentHwm);
            try (ResultSet rs = stmt.executeQuery()) {
                GapCheckResult result = processMessages(rs, currentHwm);
                if (result.lastContiguousIdn > currentHwm) {
                    LOGGER.log(Level.INFO, "Updating HWM from {0} to {1} for subscriber {2}",
                            new Object[] { currentHwm, result.lastContiguousIdn, subscriberName });
                    updateHwm(subscriberName, currentHwm, result.lastContiguousIdn);
                }
                if (!result.gapFound) {
                    LOGGER.log(Level.INFO, "No sequence gaps found after HWM for subscriber {0}", subscriberName);
                }
                return result.gapFound;
            }
        }
    }
}