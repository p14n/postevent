package com.p14n.postevent.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Responsible for finding all unprocessed events in the messages table.
 * Unprocessed events have a status of 'u'.
 */
public class UnprocessedEventFinder {
    private static final Logger LOGGER = Logger.getLogger(UnprocessedEventFinder.class.getName());

    /**
     * Finds all unprocessed events in the messages table.
     * 
     * @param connection Database connection to use
     * @return List of unprocessed events ordered by creation time (ascending)
     * @throws SQLException if a database error occurs
     */
    public List<Event> findUnprocessedEvents(Connection connection) throws SQLException {
        LOGGER.info("Finding all unprocessed events");

        String sql = "SELECT id, source, type, datacontenttype, dataschema, subject, data, " +
                "time, idn FROM postevent.messages " +
                "WHERE status = 'u' " +
                "ORDER BY time ASC";

        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            List<Event> events = processResultSet(rs);
            LOGGER.info("Found " + events.size() + " unprocessed events");
            return events;
        }
    }

    /**
     * Finds unprocessed events for a specific subject.
     * 
     * @param connection Database connection to use
     * @param subject    Subject to filter by
     * @return List of unprocessed events for the subject ordered by creation time
     *         (ascending)
     * @throws SQLException if a database error occurs
     */
    public List<Event> findUnprocessedEventsBySubject(Connection connection, String subject) throws SQLException {
        LOGGER.info("Finding unprocessed events for subject: " + subject);

        String sql = "SELECT id, source, type, datacontenttype, dataschema, subject, data, " +
                "time, idn FROM postevent.messages " +
                "WHERE status = 'u' AND subject = ? " +
                "ORDER BY time ASC";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, subject);

            try (ResultSet rs = stmt.executeQuery()) {
                List<Event> events = processResultSet(rs);
                LOGGER.info("Found " + events.size() + " unprocessed events for subject: " + subject);
                return events;
            }
        }
    }

    /**
     * Finds a limited number of unprocessed events.
     * 
     * @param connection Database connection to use
     * @param limit      Maximum number of events to return
     * @return List of unprocessed events ordered by creation time (ascending),
     *         limited to the specified count
     * @throws SQLException if a database error occurs
     */
    public List<Event> findUnprocessedEventsWithLimit(Connection connection, int limit) throws SQLException {
        LOGGER.info("Finding up to " + limit + " unprocessed events");

        String sql = "SELECT id, source, type, datacontenttype, dataschema, subject, data, " +
                "time, idn FROM postevent.messages " +
                "WHERE status = 'u' " +
                "ORDER BY time ASC " +
                "LIMIT ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, limit);

            try (ResultSet rs = stmt.executeQuery()) {
                List<Event> events = processResultSet(rs);
                LOGGER.info("Found " + events.size() + " unprocessed events");
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

        return new Event(
                id,
                source,
                type,
                dataContentType,
                dataSchema,
                subject,
                data,
                time,
                idn);
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
                LOGGER.info("Found " + count + " unprocessed events");
                return count;
            }

            return 0;
        }
    }
}