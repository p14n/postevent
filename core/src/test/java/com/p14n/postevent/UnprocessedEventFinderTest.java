package com.p14n.postevent;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.data.UnprocessedEventFinder;
import com.p14n.postevent.db.DatabaseSetup;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class UnprocessedEventFinderTest {

    private EmbeddedPostgres pg;
    private Connection connection;
    private UnprocessedEventFinder eventFinder;
    private DatabaseSetup databaseSetup;

    @BeforeEach
    public void setUp() throws Exception {
        // Start embedded PostgreSQL
        pg = EmbeddedPostgres.start();
        String jdbcUrl = pg.getJdbcUrl("postgres", "postgres");

        // Setup database schema and tables
        databaseSetup = new DatabaseSetup(jdbcUrl, "postgres", "postgres");
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createMessagesTableIfNotExists();

        // Get connection
        connection = pg.getPostgresDatabase().getConnection();

        // Create finder instance
        eventFinder = new UnprocessedEventFinder();

        // Clear any existing data
        clearTable();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (pg != null) {
            pg.close();
        }
    }

    @Test
    public void testFindUnprocessedEvents() throws SQLException {
        // Insert test events
        insertEvent("subject1", "u", 1);
        insertEvent("subject2", "u", 2);
        insertEvent("subject3", "p", 3); // This one is processed

        // Call the method
        List<Event> events = eventFinder.findUnprocessedEvents(connection);

        // Verify result
        assertEquals(2, events.size());
        assertEquals("subject1", events.get(0).subject());
        assertEquals("subject2", events.get(1).subject());
    }

    @Test
    public void testFindUnprocessedEventsBySubject() throws SQLException {
        // Insert test events
        insertEvent("subject1", "u", 1);
        insertEvent("subject1", "u", 2);
        insertEvent("subject2", "u", 3);
        insertEvent("subject1", "p", 4); // This one is processed

        // Call the method
        List<Event> events = eventFinder.findUnprocessedEventsBySubject(connection, "subject1");

        // Verify result
        assertEquals(2, events.size());
        assertEquals("subject1", events.get(0).subject());
        assertEquals("subject1", events.get(1).subject());
        assertEquals(1, events.get(0).idn());
        assertEquals(2, events.get(1).idn());
    }

    @Test
    public void testFindUnprocessedEventsWithLimit() throws SQLException {
        // Insert test events
        insertEvent("subject1", "u", 1);
        insertEvent("subject2", "u", 2);
        insertEvent("subject3", "u", 3);
        insertEvent("subject4", "u", 4);
        insertEvent("subject5", "u", 5);

        // Call the method with limit 3
        List<Event> events = eventFinder.findUnprocessedEventsWithLimit(connection, 3);

        // Verify result
        assertEquals(3, events.size());
        assertEquals("subject1", events.get(0).subject());
        assertEquals("subject2", events.get(1).subject());
        assertEquals("subject3", events.get(2).subject());
    }

    @Test
    public void testCountUnprocessedEvents() throws SQLException {
        // Insert test events
        insertEvent("subject1", "u", 1);
        insertEvent("subject2", "u", 2);
        insertEvent("subject3", "p", 3); // This one is processed

        // Call the method
        int count = eventFinder.countUnprocessedEvents(connection);

        // Verify result
        assertEquals(2, count);
    }

    @Test
    public void testFindUnprocessedEventsNoResults() throws SQLException {
        // Insert only processed events
        insertEvent("subject1", "p", 1);
        insertEvent("subject2", "p", 2);

        // Call the method
        List<Event> events = eventFinder.findUnprocessedEvents(connection);

        // Verify result
        assertTrue(events.isEmpty());
    }

    @Test
    public void testEventsOrderedByCreationTime() throws SQLException {
        // Insert events with different creation times
        insertEventWithTime("subject1", "u", 1, Instant.now().minusSeconds(60)); // 1 minute ago
        insertEventWithTime("subject2", "u", 2, Instant.now().minusSeconds(30)); // 30 seconds ago
        insertEventWithTime("subject3", "u", 3, Instant.now()); // Now

        // Call the method
        List<Event> events = eventFinder.findUnprocessedEvents(connection);

        // Verify result
        assertEquals(3, events.size());
        assertEquals("subject1", events.get(0).subject());
        assertEquals("subject2", events.get(1).subject());
        assertEquals("subject3", events.get(2).subject());
    }

    @Test
    public void testEventDataMapping() throws SQLException {
        // Insert an event with specific data
        String id = UUID.randomUUID().toString();
        String source = "test-source";
        String type = "test-type";
        String contentType = "application/json";
        String dataSchema = "test-schema";
        String subject = "test-subject";
        byte[] data = "{\"key\":\"value\"}".getBytes();
        long idn = 123L;
        String topic = "test_events";

        insertEventWithDetails(id, source, type, contentType, dataSchema, subject, data, Instant.now(), idn, "u", topic);

        // Call the method
        List<Event> events = eventFinder.findUnprocessedEvents(connection);

        // Verify result
        assertEquals(1, events.size());
        Event event = events.get(0);
        assertEquals(id, event.id());
        assertEquals(source, event.source());
        assertEquals(type, event.type());
        assertEquals(contentType, event.datacontenttype());
        assertEquals(dataSchema, event.dataschema());
        assertEquals(subject, event.subject());
        assertArrayEquals(data, event.data());
        assertEquals(idn, event.idn());
        assertNotNull(event.time());
    }

    /**
     * Clears the messages table
     */
    private void clearTable() throws SQLException {
        String sql = "DELETE FROM postevent.messages";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.executeUpdate();
        }
    }

    /**
     * Inserts a test event with the given subject, status, and idn
     */
    private void insertEvent(String subject, String status, long idn) throws SQLException {
        insertEventWithTime(subject, status, idn, Instant.now());
    }

    /**
     * Inserts a test event with the given subject, status, idn, and creation time
     */
    private void insertEventWithTime(String subject, String status, long idn, Instant createdAt) throws SQLException {
        String id = UUID.randomUUID().toString();
        String source = "test-source";
        String type = "test-type";
        String contentType = "application/json";
        String dataSchema = "test-schema";
        byte[] data = "{\"key\":\"value\"}".getBytes();
        String topic = "test_events";

        insertEventWithDetails(id, source, type, contentType, dataSchema, subject, data, createdAt, idn, status, topic);
    }

    /**
     * Inserts a test event with all details specified
     */
    private void insertEventWithDetails(String id, String source, String type, String contentType,
            String dataSchema, String subject, byte[] data,
            Instant createdAt, long idn, String status, String topic) throws SQLException {
        String sql = "INSERT INTO postevent.messages " +
                "(id, source, type, datacontenttype, dataschema, subject, data, time, idn, status, topic) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setString(2, source);
            stmt.setString(3, type);
            stmt.setString(4, contentType);
            stmt.setString(5, dataSchema);
            stmt.setString(6, subject);
            stmt.setBytes(7, data);
            stmt.setTimestamp(8, Timestamp.from(createdAt));
            stmt.setLong(9, idn);
            stmt.setString(10, status);
            stmt.setString(11, topic);

            stmt.executeUpdate();
        }
    }
}