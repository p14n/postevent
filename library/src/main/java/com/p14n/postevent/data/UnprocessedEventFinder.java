package com.p14n.postevent.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for finding unprocessed events in the messages table.
 * Provides methods to query and retrieve events with a status of 'u'
 * (unprocessed)
 * using various filtering criteria.
 *
 * <p>
 * The finder uses prepared statements for secure SQL execution and includes
 * logging for operational visibility. All database operations require an
 * external
 * {@link Connection} to be provided.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * UnprocessedEventFinder finder = new UnprocessedEventFinder();
 * try (Connection conn = dataSource.getConnection()) {
 *     List<Event> events = finder.findUnprocessedEvents(conn);
 *     // Process the events...
 * }
 * }</pre>
 */
public class UnprocessedEventFinder {

    private static final Logger logger = LoggerFactory.getLogger(UnprocessedEventFinder.class);

    /**
     * Creates a new UnprocessedEventFinder instance.
     */
    public UnprocessedEventFinder() {
    }

    /**
     * Finds all unprocessed events in the messages table.
     * Events are ordered by their sequence number (idn) in ascending order.
     * 
     * @param connection Database connection to use
     * @return List of unprocessed events
     * @throws SQLException if a database error occurs
     */
    public List<Event> findUnprocessedEvents(Connection connection) throws SQLException {
        logger.atInfo().log("Finding all unprocessed events");

        String sql = "SELECT id, source, type, datacontenttype, dataschema, subject, data, " +
                "time, idn, topic, traceparent FROM postevent.messages " +
                "WHERE status = 'u' " +
                "ORDER BY idn ASC";

        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            List<Event> events = processResultSet(rs);
            logger.atInfo().log("Found {} unprocessed events", events.size());
            return events;
        }
    }

    /**
     * Finds unprocessed events for a specific subject.
     * Events are ordered by creation time in ascending order.
     * 
     * @param connection Database connection to use
     * @param subject    Subject to filter by
     * @return List of unprocessed events for the subject
     * @throws SQLException if a database error occurs
     */
    public List<Event> findUnprocessedEventsBySubject(Connection connection, String subject) throws SQLException {
        logger.atInfo().log("Finding unprocessed events for subject: {}", subject);

        String sql = "SELECT id, source, type, datacontenttype, dataschema, subject, data, " +
                "time, idn, topic, traceparent FROM postevent.messages " +
                "WHERE status = 'u' AND subject = ? " +
                "ORDER BY time ASC";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, subject);

            try (ResultSet rs = stmt.executeQuery()) {
                List<Event> events = processResultSet(rs);
                logger.atInfo().log("Found {} unprocessed events for subject: {}", events.size(), subject);
                return events;
            }
        }
    }

    /**
     * Finds a limited number of unprocessed events.
     * Events are ordered by creation time in ascending order.
     * 
     * @param connection Database connection to use
     * @param limit      Maximum number of events to return
     * @return List of unprocessed events, limited to the specified count
     * @throws SQLException             if a database error occurs
     * @throws IllegalArgumentException if limit is less than or equal to 0
     */
    public List<Event> findUnprocessedEventsWithLimit(Connection connection, int limit) throws SQLException {
        logger.atInfo().log("Finding up to {} unprocessed events", limit);

        String sql = "SELECT id, source, type, datacontenttype, dataschema, subject, data, " +
                "time, idn, topic, traceparent FROM postevent.messages " +
                "WHERE status = 'u' " +
                "ORDER BY time ASC " +
                "LIMIT ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, limit);

            try (ResultSet rs = stmt.executeQuery()) {
                List<Event> events = processResultSet(rs);
                logger.atInfo().log("Found {} unprocessed events", events.size());
                return events;
            }
        }
    }

    /**
     * Processes a result set and converts each row to an Event object.
     * 
     * @param rs Result set to process
     * @return List of Event objects created from the result set
     * @throws SQLException if a database error occurs
     */
    private List<Event> processResultSet(ResultSet rs) throws SQLException {
        List<Event> events = new ArrayList<>();

        while (rs.next()) {
            Event event = mapResultSetToEvent(rs);
            events.add(event);
        }

        return events;
    }

    /**
     * Maps a database result set row to an Event object.
     * 
     * @param rs Result set positioned at the row to map
     * @return Event object created from the result set data
     * @throws SQLException if a database error occurs
     */
    private Event mapResultSetToEvent(ResultSet rs) throws SQLException {
        String id = rs.getString("id");
        String source = rs.getString("source");
        String type = rs.getString("type");
        String dataContentType = rs.getString("datacontenttype");
        String dataSchema = rs.getString("dataschema");
        String subject = rs.getString("subject");
        byte[] data = rs.getBytes("data");
        Instant time = rs.getTimestamp("time").toInstant();
        long idn = rs.getLong("idn");
        String topic = rs.getString("topic");
        String traceparent = rs.getString("traceparent");

        return Event.create(
                id,
                source,
                type,
                dataContentType,
                dataSchema,
                subject,
                data,
                time,
                idn,
                topic,
                traceparent);
    }

    /**
     * Counts the number of unprocessed events in the messages table.
     * 
     * @param connection Database connection to use
     * @return Count of unprocessed events
     * @throws SQLException if a database error occurs
     */
    public int countUnprocessedEvents(Connection connection) throws SQLException {
        String sql = "SELECT COUNT(*) FROM postevent.messages WHERE status = 'u'";

        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                int count = rs.getInt(1);
                logger.atInfo().log("Found {} unprocessed events", count);
                return count;
            }

            return 0;
        }
    }
}
