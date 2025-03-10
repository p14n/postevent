package com.p14n.postevent;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CatchupServiceTest {

    private static final Logger log = LoggerFactory.getLogger(CatchupServiceTest.class);
    private static final String TEST_TOPIC = "test_events";
    private static final String SUBSCRIBER_NAME = "test_subscriber";

    private EmbeddedPostgres pg;
    private Connection connection;
    private Publisher publisher;
    private CatchupServer catchupServer;
    private CatchupService catchupService;
    private PersistentSubscriber persistentSubscriber;

    @BeforeEach
    public void setup() throws Exception {
        // Start embedded PostgreSQL
        pg = EmbeddedPostgres.start();
        String jdbcUrl = pg.getJdbcUrl("postgres", "postgres");
        connection = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");

        // Setup database schema and tables
        DatabaseSetup setup = new DatabaseSetup(jdbcUrl, "postgres", "postgres");
        setup.createSchemaIfNotExists()
                .createTableIfNotExists(TEST_TOPIC)
                .createMessagesTableIfNotExists()
                .createContiguousHwmTableIfNotExists();

        // Initialize components
        publisher = new Publisher();
        catchupServer = new CatchupServer(TEST_TOPIC, pg.getPostgresDatabase());
        catchupService = new CatchupService(connection, catchupServer, TEST_TOPIC);
        persistentSubscriber = new PersistentSubscriber(new MessageSubscriber<Event>() {
            @Override
            public void onMessage(Event event) {
                // Do nothing
            }

            @Override
            public void onError(Throwable t) {
                // Do nothing
            }
        }, pg.getPostgresDatabase());
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        if (pg != null) {
            pg.close();
        }
    }

    private void createProcessingGap() throws Exception {
        connection.createStatement().execute("""
                INSERT INTO postevent.messages (id, source, datacontenttype, dataschema, subject, data, idn)
                select id, source, datacontenttype, dataschema, subject, data, idn
                from postevent.test_events
                where idn = (select max(idn) from postevent.test_events)
                """);
    }

    private void copyEventsToMessages() throws Exception {
        connection.createStatement().execute("""
                INSERT INTO postevent.messages (id, source, datacontenttype, dataschema, subject, data, idn)
                select id, source, datacontenttype, dataschema, subject, data, idn
                from postevent.test_events
                where idn = (select max(idn) from postevent.test_events)
                """);
    }

    @Test
    public void testCatchupProcessesNewEvents() throws Exception {
        // Publish some test events
        List<Event> publishedEvents = new ArrayList<>();
        for (int i = 1; i < 26; i++) {
            Event event = new Event(
                    UUID.randomUUID().toString(),
                    "test-source",
                    "test-type",
                    "application/json",
                    null,
                    "test-subject",
                    ("{\"value\":" + i + "}").getBytes(),
                    null);
            publisher.publish(event, connection, TEST_TOPIC);
            publishedEvents.add(event);
        }

        createProcessingGap();

        // First catchup should process events
        int processedCount = catchupService.catchup(SUBSCRIBER_NAME, 20);
        assertTrue(processedCount > 0, "Should have processed some events");

        // Verify HWM was updated
        long hwm = getCurrentHwm(SUBSCRIBER_NAME);
        assertTrue(hwm > 0, "HWM should have been updated");

        // Verify messages were written to messages table
        int messagesCount = countMessagesInTable();
        assertEquals(processedCount, messagesCount - 1,
                "Number of messages in table should match processed count");

        // Second catchup should process remaining events
        int secondProcessedCount = catchupService.catchup(SUBSCRIBER_NAME, 20);
        assertTrue(secondProcessedCount > 0,
                "Should have processed remaining events");

        // Verify all events were processed
        int totalMessagesCount = countMessagesInTable();
        assertEquals(publishedEvents.size(), totalMessagesCount,
                "All events should have been processed");

        // Third catchup should process no events
        int thirdProcessedCount = catchupService.catchup(SUBSCRIBER_NAME, 20);
        assertEquals(0, thirdProcessedCount, "No more events should be processed");
    }

    @Test
    public void testCatchupWithExistingHwm() throws Exception {
        // Publish some initial events
        log.debug("Publishing initial 10 events");
        for (int i = 1; i < 11; i++) {
            Event event = new Event(
                    UUID.randomUUID().toString(),
                    "test-source",
                    "test-type",
                    "application/json",
                    null,
                    "test-subject",
                    ("{\"value\":" + i + "}").getBytes(),
                    null);
            publisher.publish(event, connection, TEST_TOPIC);
        }

        createProcessingGap();
        logEventsInTopicTable();
        logEventsInMessagesTable();
        // Process initial events
        log.debug("Processing initial events");
        int initialProcessed = catchupService.catchup(SUBSCRIBER_NAME, 20);
        long initialHwm = getCurrentHwm(SUBSCRIBER_NAME);
        log.debug("Initial processing complete: processed {} events, HWM = {}",
                initialProcessed, initialHwm);

        // Publish more events
        log.debug("Publishing 5 more events");
        for (int i = 11; i < 16; i++) {
            Event event = new Event(
                    UUID.randomUUID().toString(),
                    "test-source",
                    "test-type",
                    "application/json",
                    null,
                    "test-subject",
                    ("{\"value\":" + i + "}").getBytes(),
                    null);
            publisher.publish(event, connection, TEST_TOPIC);
        }

        createProcessingGap();
        // Process new events
        log.debug("Processing new events");
        int processedCount = catchupService.catchup(SUBSCRIBER_NAME, 20);
        long newHwm = getCurrentHwm(SUBSCRIBER_NAME);
        log.debug("New processing complete: processed {} events, HWM = {}",
                processedCount, newHwm);

        // Log the actual events in the table for debugging
        log.debug("Checking events in the table:");
        logEventsInMessagesTable();

        // There should be 4 new events in the messages table as the processing gap
        // created a gap of 4 events
        assertEquals(4, processedCount, "Should have processed 4 new events");

        // Verify HWM was updated
        assertTrue(newHwm > initialHwm, "HWM should have increased");
    }

    @Test
    public void testHasSequenceGapWithNoGap() throws Exception {
        // Publish sequential events
        log.debug("Publishing 5 sequential events");
        for (int i = 0; i < 5; i++) {
            Event event = new Event(
                    UUID.randomUUID().toString(),
                    "test-source",
                    "test-type",
                    "application/json",
                    null,
                    "test-subject",
                    ("{\"value\":" + i + "}").getBytes(),
                    null);
            publisher.publish(event, connection, TEST_TOPIC);
        }

        copyEventsToMessages();

        // Initialize HWM to 0
        initializeHwm(SUBSCRIBER_NAME, 0);

        // Check for gaps
        boolean hasGap = catchupService.hasSequenceGap(SUBSCRIBER_NAME, 0);

        // Verify no gap was found
        assertFalse(hasGap, "Should not find any gaps in sequential events");

        // Verify HWM was updated to the last event
        long newHwm = getCurrentHwm(SUBSCRIBER_NAME);
        assertEquals(5, newHwm, "HWM should be updated to the last event");
    }

    @Test
    public void testHasSequenceGapWithGap() throws Exception {
        // Create a gap by publishing events with specific IDs
        // We'll manually insert events with IDNs 1, 2, 3, 5, 6 (gap at 4)
        log.debug("Publishing events with a gap");

        // First, insert events 1-3
        for (int i = 1; i <= 3; i++) {
            insertEventWithIdn(i);
        }

        // Skip 4 to create a gap

        // Then insert events 5-6
        for (int i = 5; i <= 6; i++) {
            insertEventWithIdn(i);
        }

        copyEventsToMessages();

        // Initialize HWM to 0
        initializeHwm(SUBSCRIBER_NAME, 0);

        // Check for gaps
        boolean hasGap = catchupService.hasSequenceGap(SUBSCRIBER_NAME, 0);

        // Verify a gap was found
        assertTrue(hasGap, "Should find a gap in the sequence");

        // Verify HWM was updated to the last event before the gap
        long newHwm = getCurrentHwm(SUBSCRIBER_NAME);
        assertEquals(3, newHwm, "HWM should be updated to the last event before the gap");
    }

    @Test
    public void testHasSequenceGapWithMultipleGaps() throws Exception {
        // Create multiple gaps: 1, 2, 4, 7, 8 (gaps at 3, 5-6)
        log.debug("Publishing events with multiple gaps");

        int[] idns = { 1, 2, 4, 7, 8 };
        for (int idn : idns) {
            insertEventWithIdn(idn);
        }

        // Initialize HWM to 0
        initializeHwm(SUBSCRIBER_NAME, 0);

        // Check for gaps
        boolean hasGap = catchupService.hasSequenceGap(SUBSCRIBER_NAME, 0);

        // Verify a gap was found
        assertTrue(hasGap, "Should find a gap in the sequence");

        // Verify HWM was updated to the last event before the first gap
        long newHwm = getCurrentHwm(SUBSCRIBER_NAME);
        assertEquals(2, newHwm, "HWM should be updated to the last event before the first gap");

        // Check for gaps again, starting from the new HWM
        hasGap = catchupService.hasSequenceGap(SUBSCRIBER_NAME, newHwm);

        // Verify a gap was found
        assertTrue(hasGap, "Should find another gap in the sequence");

        // Verify HWM was updated correctly
        newHwm = getCurrentHwm(SUBSCRIBER_NAME);
        assertEquals(4, newHwm, "HWM should remain at 4 as there's a gap after it");
    }

    /**
     * Helper method to insert an event with a specific IDN
     */
    private void insertEventWithIdn(long idn) throws SQLException {
        String sql = "INSERT INTO " + TEST_TOPIC +
                " (id, source, type, datacontenttype, subject, data, idn) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, UUID.randomUUID().toString());
            stmt.setString(2, "test-source");
            stmt.setString(3, "test-type");
            stmt.setString(4, "application/json");
            stmt.setString(5, "test-subject");
            stmt.setBytes(6, ("{\"value\":" + idn + "}").getBytes());
            stmt.setLong(7, idn);

            stmt.executeUpdate();
        }
    }

    /**
     * Helper method to initialize HWM for a subscriber
     */
    private void initializeHwm(String subscriberName, long hwm) throws SQLException {
        String sql = "INSERT INTO postevent.contiguous_hwm (subscriber_name, hwm) " +
                "VALUES (?, ?) " +
                "ON CONFLICT (subscriber_name) DO UPDATE SET hwm = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, subscriberName);
            stmt.setLong(2, hwm);
            stmt.setLong(3, hwm);

            stmt.executeUpdate();
        }
    }

    /**
     * Helper method to get current HWM for a subscriber
     */
    private long getCurrentHwm(String subscriberName) throws SQLException {
        String sql = "SELECT hwm FROM postevent.contiguous_hwm WHERE subscriber_name = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, subscriberName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("hwm");
                } else {
                    return 0;
                }
            }
        }
    }

    private int countMessagesInTable() throws Exception {
        String sql = "SELECT COUNT(*) FROM postevent.messages";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    private void logEventsInMessagesTable() throws Exception {
        log.debug("postevent.messages contents:");
        String sql = "SELECT idn, id, source FROM postevent.messages ORDER BY idn";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                log.debug("Event: idn={}, id={}, source={}",
                        rs.getLong("idn"),
                        rs.getString("id"),
                        rs.getString("source"));
            }
        }
    }

    private void logEventsInTopicTable() throws Exception {
        log.debug("postevent.test_events contents:");
        String sql = "SELECT idn, id, source FROM postevent.test_events ORDER BY idn";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                log.debug("Event: idn={}, id={}, source={}",
                        rs.getLong("idn"),
                        rs.getString("id"),
                        rs.getString("source"));
            }
        }
    }

}