package com.p14n.postevent.catchup;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.SQL;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Database implementation of the CatchupServerInterface.
 * Provides functionality to fetch missed events and latest message IDs from a
 * PostgreSQL database.
 * Uses a DataSource for connection management and prepared statements for
 * secure SQL execution.
 *
 * <p>
 * Events are stored in topic-specific tables within the 'postevent' schema.
 * Each table
 * contains events with sequential IDs (idn) that can be used for range-based
 * queries.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * DataSource dataSource = // initialize your datasource
 * CatchupServer server = new CatchupServer(dataSource);
 * 
 * // Fetch up to 100 events from sequence 1000 to 2000
 * List<Event> events = server.fetchEvents(1000L, 2000L, 100, "my-topic");
 * 
 * // Get the latest event ID
 * long latestId = server.getLatestMessageId("my-topic");
 * }</pre>
 */
public class CatchupServer implements CatchupServerInterface {
    private static final Logger logger = LoggerFactory.getLogger(CatchupServer.class);

    private final DataSource dataSource;

    /**
     * Creates a new CatchupServer instance.
     *
     * @param dataSource The DataSource to use for database connections.
     *                   Should be properly configured with connection pooling and
     *                   credentials.
     */
    public CatchupServer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * This implementation fetches events from a PostgreSQL database using a
     * prepared statement
     * to prevent SQL injection. Events are retrieved in order by their sequence
     * number (idn).
     * </p>
     *
     * <p>
     * The SQL query used is:
     * {@code SELECT * FROM postevent.<topic> WHERE idn BETWEEN (?+1) AND ? ORDER BY idn LIMIT ?}
     * </p>
     *
     * @throws IllegalArgumentException if startAfter is greater than end,
     *                                  maxResults is less than or equal to 0,
     *                                  or if topic is null or empty
     * @throws RuntimeException         if there is a database access error
     */
    @Override
    public List<Event> fetchEvents(long startAfter, long end, int maxResults, String topic) {
        if (startAfter > end) {
            throw new IllegalArgumentException("Start value must be less than or equal to end value");
        }
        if (maxResults <= 0) {
            throw new IllegalArgumentException("Max results must be greater than zero");
        }
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }

        List<Event> events = new ArrayList<>();
        String sql = String.format(
                "SELECT * FROM postevent.%s WHERE idn BETWEEN (?+1) AND ? ORDER BY idn LIMIT ?",
                topic);

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, startAfter);
            stmt.setLong(2, end);
            stmt.setInt(3, maxResults);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Event event = SQL.eventFromResultSet(rs, topic);
                    events.add(event);
                }
            }

            logger.atInfo().log(String.format("Fetched %d events from topic %s between %d and %d",
                    events.size(), topic, startAfter, end));

            return events;

        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error fetching events from database");
            throw new RuntimeException("Failed to fetch events", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * This implementation queries the PostgreSQL database for the maximum sequence
     * number (idn)
     * in the specified topic's table. Returns 0 if no events exist.
     * </p>
     *
     * <p>
     * The SQL query used is: {@code SELECT MAX(idn) FROM postevent.<topic>}
     * </p>
     *
     * @throws IllegalArgumentException if topic is null or empty
     * @throws RuntimeException         if there is a database access error
     */
    @Override
    public long getLatestMessageId(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }

        String sql = String.format("SELECT MAX(idn) FROM postevent.%s", topic);

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;

        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error fetching latest message ID");
            throw new RuntimeException("Failed to fetch latest message ID", e);
        }
    }
}
