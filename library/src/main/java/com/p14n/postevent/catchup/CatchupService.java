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
 * Service responsible for managing event catchup operations and maintaining
 * high water marks (HWM).
 * Implements both MessageSubscriber for SystemEvents and OneAtATimeBehaviour
 * for sequential processing.
 * 
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Event catchup processing for missed events</li>
 * <li>High water mark (HWM) management</li>
 * <li>Gap detection in event sequences</li>
 * <li>Sequential processing of catchup operations</li>
 * </ul>
 * 
 * <p>
 * The service responds to two types of system events:
 * </p>
 * <ul>
 * <li>{@code SystemEvent.CatchupRequired}: Triggers full catchup
 * processing</li>
 * <li>{@code SystemEvent.FetchLatest}: Fetches only the latest events</li>
 * </ul>
 */
public class CatchupService implements MessageSubscriber<SystemEvent>, OneAtATimeBehaviour {
    private static final Logger LOGGER = LoggerFactory.getLogger(CatchupService.class);
    private static final int DEFAULT_BATCH_SIZE = 20;

    private final CatchupServerInterface catchupServer;
    private final DataSource datasource;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private final SystemEventBroker systemEventBroker;
    final AtomicInteger signals = new AtomicInteger(0);
    final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Creates a new CatchupService instance.
     *
     * @param ds                The DataSource for database operations
     * @param catchupServer     The server interface for fetching catchup events
     * @param systemEventBroker The broker for publishing system events
     */
    public CatchupService(DataSource ds, CatchupServerInterface catchupServer, SystemEventBroker systemEventBroker) {
        this.datasource = ds;
        this.catchupServer = catchupServer;
        this.systemEventBroker = systemEventBroker;
    }

    /**
     * Performs catchup processing for a specific topic.
     * Fetches missed events between the current high water mark and the next gap,
     * writes them to the messages table, and updates the HWM accordingly.
     * 
     * @param topicName The topic to process catchup for
     * @return The number of events processed during catchup
     */
    public int catchup(String topicName) {

        try (Connection conn = datasource.getConnection()) {
            conn.setAutoCommit(false);

            long currentHwm = getCurrentHwm(conn, topicName);

            // Find the next gap end (lowest idn greater than hwm)
            long gapEnd = findGapEnd(conn, currentHwm, topicName);

            LOGGER.info(String.format("Current HWM %d highest message in gap %d",
                    currentHwm, gapEnd));

            if (gapEnd <= (currentHwm + 1)) {
                LOGGER.info("No new gap events to process for topic: " + topicName);
                if (updateHwmToLastContiguous(topicName, currentHwm, conn)) {
                    systemEventBroker.publish(SystemEvent.UnprocessedCheckRequired);
                }
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
            if (events.size() == batchSize || events.size() == gapEnd - currentHwm) {
                systemEventBroker.publish(SystemEvent.CatchupRequired.withTopic(topicName));
            } else {
                systemEventBroker.publish(SystemEvent.UnprocessedCheckRequired);
            }
            return processedCount;
        } catch (SQLException e) {
            LOGGER.error("Failed to catch up", e);
            return 0;
        }
    }

    /**
     * Gets the current high water mark (HWM) for a topic from the contiguous_hwm
     * table.
     * If no HWM exists for the topic, initializes it with 0.
     *
     * @param connection Database connection
     * @param topicName  Topic to get HWM for
     * @return Current HWM value
     * @throws SQLException If database operation fails
     */
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

    /**
     * Initializes the HWM for a topic with value 0.
     *
     * @param connection Database connection
     * @param topicName  Topic to initialize
     * @throws SQLException If database operation fails
     */
    private void initializeHwm(Connection connection, String topicName) throws SQLException {
        String sql = "INSERT INTO postevent.contiguous_hwm (topic_name, hwm) VALUES (?, 0) ON CONFLICT DO NOTHING";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topicName);
            stmt.executeUpdate();
        }
        getCurrentHwm(connection, topicName);
    }

    /**
     * Finds the next gap end (lowest idn greater than current HWM) for a topic.
     *
     * @param connection Database connection
     * @param currentHwm Current HWM value
     * @param topicName  Topic to check
     * @return The ID of the next message after the gap, or currentHwm if no gap
     *         exists
     * @throws SQLException If database operation fails
     */
    private long findGapEnd(Connection connection, long currentHwm, String topicName) throws SQLException {
        String sql = "SELECT MIN(idn) as next_idn FROM postevent.messages WHERE topic=? AND idn > ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topicName);
            stmt.setLong(2, currentHwm);

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

    /**
     * Writes a batch of events to the messages table.
     *
     * @param connection Database connection
     * @param events     List of events to write
     * @return Number of events successfully written
     * @throws SQLException If database operation fails
     */
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

    /**
     * Updates the high water mark for a topic using optimistic locking.
     * Only updates if the current HWM matches the expected value.
     *
     * @param connection Database connection
     * @param topicName  Topic to update
     * @param currentHwm Expected current HWM value
     * @param newHwm     New HWM value to set
     * @throws SQLException If database operation fails
     */
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
            }
        }
    }

    /**
     * Handles incoming system events, processing them one at a time.
     * Supports CatchupRequired and FetchLatest events.
     *
     * @param message The system event to process
     */
    @Override
    public void onMessage(SystemEvent message) {
        if (Objects.requireNonNull(message) == SystemEvent.CatchupRequired) {
            oneAtATime(() -> catchup(message.topic), () -> onMessage(message));
        } else if (message == SystemEvent.FetchLatest) {
            oneAtATime(() -> fetchLatest(message.topic), () -> onMessage(message));
        }
    }

    /**
     * Fetches and processes the latest event for a topic.
     * If successful, triggers a catchup to process any intermediate events.
     *
     * @param topicName Topic to fetch latest event for
     * @return Number of events processed (0 or 1)
     */
    private int fetchLatest(String topicName) {
        if (topicName == null) {
            LOGGER.warn("Topic name is null for fetch latest");
            return 0;
        }

        try (Connection conn = datasource.getConnection()) {
            conn.setAutoCommit(false);

            long currentHwm = getCurrentHwm(conn, topicName);

            // Get the latest message ID from the server
            long latestMessageId = catchupServer.getLatestMessageId(topicName);

            if (latestMessageId <= currentHwm) {
                LOGGER.info("No new messages found after HWM {} for topic {}", currentHwm, topicName);
                return 0;
            }

            // Fetch just the latest message
            List<Event> events = catchupServer.fetchEvents(latestMessageId - 1, latestMessageId, 1, topicName);

            if (events.isEmpty()) {
                LOGGER.info("No events found in range for topic: " + topicName);
                return 0;
            }

            // Write event to messages table
            int processedCount = writeEventsToMessagesTable(conn, events);

            LOGGER.info("Processed latest event for topic {}",
                    topicName);

            conn.commit();

            // If there are more messages between currentHwm and latestMessageId,
            // trigger a catchup to get the rest
            systemEventBroker.publish(SystemEvent.CatchupRequired.withTopic(topicName));

            return processedCount;
        } catch (SQLException e) {
            LOGGER.error("Failed to fetch latest", e);
            return 0;
        }
    }

    private long findLatestMessageId(Connection connection, String topicName) throws SQLException {
        String sql = "SELECT MAX(idn) FROM postevent." + topicName;
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return 0;
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

    /**
     * Processes a result set to check for gaps in message sequence.
     * Updates the last contiguous IDN as it processes messages.
     *
     * @param rs         ResultSet containing message IDNs
     * @param currentHwm Current HWM value to start checking from
     * @return GapCheckResult containing gap status and last contiguous IDN
     * @throws SQLException If database operation fails
     */
    private GapCheckResult processMessages(ResultSet rs, long currentHwm) throws SQLException {
        long expectedNext = currentHwm + 1;
        long lastContiguousIdn = currentHwm;
        while (rs.next()) {
            long actualIdn = rs.getLong("idn");
            if (actualIdn > expectedNext) {
                LOGGER.info("Gap found: Expected {}, found {} (gap of {})",
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
     * @param topicName  Topic to check
     * @param currentHwm Current HWM value
     * @return true if a gap was found, false if no gaps were found
     * @throws SQLException If database operation fails
     */
    public boolean hasSequenceGap(String topicName, long currentHwm) throws SQLException {

        try (Connection connection = datasource.getConnection()) {

            connection.setAutoCommit(false);
            var result = hasSequenceGap(topicName, currentHwm, connection);
            connection.commit();
            return result;
        }
    }

    /**
     * Internal version of hasSequenceGap that accepts an existing connection.
     *
     * @param topicName  Topic to check
     * @param currentHwm Current HWM value
     * @param connection Existing database connection to use
     * @return true if a gap was found, false if no gaps were found
     * @throws SQLException If database operation fails
     */
    public boolean hasSequenceGap(String topicName, long currentHwm, Connection connection) throws SQLException {
        LOGGER.info("Checking for sequence gaps after HWM {} for topic {}",
                new Object[] { currentHwm, topicName });
        String sql = "SELECT idn FROM postevent.messages WHERE idn > ? ORDER BY idn";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {

            stmt.setLong(1, currentHwm);
            try (ResultSet rs = stmt.executeQuery()) {
                GapCheckResult result = processMessages(rs, currentHwm);
                if (result.lastContiguousIdn > currentHwm) {
                    LOGGER.info("Updating HWM from {} to {} for topic {}",
                            new Object[] { currentHwm, result.lastContiguousIdn, topicName });
                    updateHwm(connection, topicName, currentHwm, result.lastContiguousIdn);
                }
                if (!result.gapFound) {
                    LOGGER.info("No sequence gaps found after HWM for topic {}",
                            new Object[] { topicName });
                }
                connection.commit();
                return result.gapFound;
            }
        }
    }

    /**
     * Updates the HWM to the last contiguous message ID for a topic.
     *
     * @param topicName  Topic to update
     * @param currentHwm Current HWM value
     * @param connection Database connection
     * @return true if HWM was updated, false if no update was needed
     * @throws SQLException If database operation fails
     */
    public boolean updateHwmToLastContiguous(String topicName, long currentHwm, Connection connection)
            throws SQLException {
        String sql = "SELECT idn FROM postevent.messages WHERE topic = ? AND idn > ? ORDER BY idn";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topicName);
            stmt.setLong(2, currentHwm);
            try (ResultSet rs = stmt.executeQuery()) {
                GapCheckResult result = processMessages(rs, currentHwm);
                boolean shouldUpdate = result.lastContiguousIdn > currentHwm;
                if (shouldUpdate) {
                    LOGGER.info("Updating HWM from {} to {} for topic {}",
                            new Object[] { currentHwm, result.lastContiguousIdn, topicName });
                    updateHwm(connection, topicName, currentHwm, result.lastContiguousIdn);
                }
                connection.commit();
                return shouldUpdate;
            }
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
