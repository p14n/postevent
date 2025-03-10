package com.p14n.postevent;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CatchupServiceTest {

    private static final String TEST_TOPIC = "test_events";
    private static final String SUBSCRIBER_NAME = "test_subscriber";

    private EmbeddedPostgres pg;
    private Connection connection;
    private Publisher publisher;
    private CatchupServer catchupServer;
    private CatchupService catchupService;

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

    @Test
    public void testCatchupProcessesNewEvents() throws Exception {
        // Publish some test events
        List<Event> publishedEvents = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
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

        // First catchup should process events
        int processedCount = catchupService.catchup(SUBSCRIBER_NAME, 20);
        assertTrue(processedCount > 0, "Should have processed some events");

        // Verify HWM was updated
        long hwm = getCurrentHwm(SUBSCRIBER_NAME);
        assertTrue(hwm > 0, "HWM should have been updated");

        // Verify messages were written to messages table
        int messagesCount = countMessagesInTable();
        assertEquals(processedCount, messagesCount, "Number of messages in table should match processed count");

        // Second catchup should process remaining events
        int secondProcessedCount = catchupService.catchup(SUBSCRIBER_NAME, 20);
        assertTrue(secondProcessedCount > 0, "Should have processed remaining events");

        // Verify all events were processed
        int totalMessagesCount = countMessagesInTable();
        assertEquals(publishedEvents.size(), totalMessagesCount, "All events should have been processed");

        // Third catchup should process no events
        int thirdProcessedCount = catchupService.catchup(SUBSCRIBER_NAME, 20);
        assertEquals(0, thirdProcessedCount, "No more events should be processed");
    }

    @Test
    public void testCatchupWithExistingHwm() throws Exception {
        // Publish some initial events
        for (int i = 0; i < 10; i++) {
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

        // Process initial events
        catchupService.catchup(SUBSCRIBER_NAME, 20);
        long initialHwm = getCurrentHwm(SUBSCRIBER_NAME);

        // Publish more events
        for (int i = 10; i < 15; i++) {
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

        // Process new events
        int processedCount = catchupService.catchup(SUBSCRIBER_NAME, 20);
        assertEquals(5, processedCount, "Should have processed 5 new events");

        // Verify HWM was updated
        long newHwm = getCurrentHwm(SUBSCRIBER_NAME);
        assertTrue(newHwm > initialHwm, "HWM should have increased");
    }

    private long getCurrentHwm(String subscriberName) throws Exception {
        String sql = "SELECT hwm FROM postevent.contiguous_hwm WHERE subscriber_name = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, subscriberName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("hwm");
                }
                return 0;
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
}