package com.p14n.postevent;

import com.p14n.postevent.broker.*;
import com.p14n.postevent.catchup.CatchupServer;
import com.p14n.postevent.catchup.CatchupService;
import com.p14n.postevent.catchup.PersistentBroker;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.DatabaseSetup;

import io.opentelemetry.api.OpenTelemetry;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.p14n.postevent.TestUtil.createTestEvent;

public class CatchupServiceTest {

    private static final Logger log = LoggerFactory.getLogger(CatchupServiceTest.class);
    private static final String TEST_TOPIC = "test_events";
    private EmbeddedPostgres pg;
    private CatchupServer catchupServer;
    private CatchupService catchupService;
    private PersistentBroker persistentBroker;

    @BeforeEach
    public void setup() throws Exception {
        // Start embedded PostgreSQL
        pg = EmbeddedPostgres.start();
        String jdbcUrl = pg.getJdbcUrl("postgres", "postgres");

        // Setup database schema and tables
        DatabaseSetup setup = new DatabaseSetup(jdbcUrl, "postgres", "postgres");
        setup.createSchemaIfNotExists()
                .createTableIfNotExists(TEST_TOPIC)
                .createMessagesTableIfNotExists()
                .createContiguousHwmTableIfNotExists();

        var ot = OpenTelemetry.noop();

        // Initialize components
        catchupServer = new CatchupServer(pg.getPostgresDatabase());
        catchupService = new CatchupService(pg.getPostgresDatabase(), catchupServer, new SystemEventBroker(ot));
        persistentBroker = new PersistentBroker<>(new EventMessageBroker(ot), pg.getPostgresDatabase(),
                new SystemEventBroker(ot));
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (pg != null) {
            pg.close();
        }
    }

    private void createProcessingGap(Connection connection) throws Exception {
        connection.createStatement().execute(
                """
                        INSERT INTO postevent.messages (id, source, type, datacontenttype, dataschema, subject, data, idn, topic)
                        select id, source, type, datacontenttype, dataschema, subject, data, idn, 'test_events'
                        from postevent.test_events
                        where idn = (select max(idn) from postevent.test_events)
                        """);
        connection.commit();
    }

    private void copyEventsToMessages(Connection connection, long lowestIdn) throws Exception {
        connection.createStatement().execute(
                """
                        INSERT INTO postevent.messages (id, source, type, datacontenttype, dataschema, subject, data, idn, topic)
                        select id, source, type, datacontenttype, dataschema, subject, data, idn, 'test_events'
                        from postevent.test_events
                        where idn >= """
                        + lowestIdn);
    }

    @Test
    public void testCatchupProcessesNewEvents() throws Exception {
        // Publish some test events
        try (Connection connection = pg.getPostgresDatabase().getConnection()) {
            connection.setAutoCommit(false);

            List<Event> publishedEvents = new ArrayList<>();
            for (int i = 1; i < 26; i++) {
                Event event = createTestEvent(i);
                Publisher.publish(event, connection, TEST_TOPIC);
                publishedEvents.add(event);
            }

            createProcessingGap(connection);

            // First catchup should process events
            int processedCount = catchupService.catchup(TEST_TOPIC);
            assertTrue(processedCount > 0, "Should have processed some events");

            // Verify HWM was updated
            long hwm = getCurrentHwm(connection, TEST_TOPIC);
            assertTrue(hwm > 0, "HWM should have been updated");

            // Verify messages were written to messages table
            int messagesCount = countMessagesInTable(connection);
            assertEquals(processedCount, messagesCount - 1,
                    "Number of messages in table should match processed count");

            // Second catchup should process remaining events
            int secondProcessedCount = catchupService.catchup(TEST_TOPIC);
            assertTrue(secondProcessedCount > 0,
                    "Should have processed remaining events");

            // Verify all events were processed
            int totalMessagesCount = countMessagesInTable(connection);
            assertEquals(publishedEvents.size(), totalMessagesCount,
                    "All events should have been processed");

            // Third catchup should process no events
            int thirdProcessedCount = catchupService.catchup(TEST_TOPIC);
            assertEquals(0, thirdProcessedCount, "No more events should be processed");

        }
    }

    @Test
    public void testCatchupWithExistingHwm() throws Exception {

        try (Connection connection = pg.getPostgresDatabase().getConnection()) {
            connection.setAutoCommit(false);
            // Publish some initial events
            log.debug("Publishing initial 10 events");
            for (int i = 1; i < 11; i++) {
                Event event = createTestEvent(i);
                Publisher.publish(event, connection, TEST_TOPIC);
            }

            createProcessingGap(connection);
            TestUtil.logEventsInTopicTable(connection, log, TEST_TOPIC);
            TestUtil.logEventsInMessagesTable(connection, log);
            // Process initial events
            log.debug("Processing initial events");
            int initialProcessed = catchupService.catchup(TEST_TOPIC);

            long initialHwm = getCurrentHwm(connection, TEST_TOPIC);
            log.debug("Initial processing complete: processed {} events, HWM = {}",
                    initialProcessed, initialHwm);

            // Publish more events
            log.debug("Publishing 5 more events");
            for (int i = 11; i < 16; i++) {
                Event event = createTestEvent(i);
                Publisher.publish(event, connection, TEST_TOPIC);
            }

            createProcessingGap(connection);
            // Process new events
            log.debug("Processing new events");
            int processedCount = catchupService.catchup(TEST_TOPIC);
            long newHwm = getCurrentHwm(connection, TEST_TOPIC);
            log.debug("New processing complete: processed {} events, HWM = {}",
                    processedCount, newHwm);

            // Log the actual events in the table for debugging
            log.debug("Checking events in the table:");
            TestUtil.logEventsInMessagesTable(connection, log);

            // There should be 4 new events in the messages table as the processing gap
            // created a gap of 4 events
            assertEquals(4, processedCount, "Should have processed 4 new events");

            // Verify HWM was updated
            assertTrue(newHwm > initialHwm, "HWM should have increased");
        }
    }

    @Test
    public void testHasSequenceGapWithNoGap() throws Exception {

        try (Connection connection = pg.getPostgresDatabase().getConnection()) {

            // Publish sequential events
            log.debug("Publishing 5 sequential events");
            for (int i = 0; i < 5; i++) {
                Event event = createTestEvent(i);
                Publisher.publish(event, connection, TEST_TOPIC);
            }

            copyEventsToMessages(connection, 0);

            // Initialize HWM to 0
            initializeHwm(connection, TEST_TOPIC, 0);

            // Check for gaps
            boolean hasGap = catchupService.hasSequenceGap(TEST_TOPIC, 0);

            // Verify no gap was found
            assertFalse(hasGap, "Should not find any gaps in sequential events");

            // Verify HWM was updated to the last event
            long newHwm = getCurrentHwm(connection, TEST_TOPIC);
            assertEquals(5, newHwm, "HWM should be updated to the last event");
        }
    }

    @Test
    public void testHasSequenceGapWithGap() throws Exception {

        try (Connection connection = pg.getPostgresDatabase().getConnection()) {

            // Create a gap by publishing events with specific IDs
            // We'll manually insert events with IDNs 1, 2, 3, 5, 6 (gap at 4)
            log.debug("Publishing events with a gap");

            // First, insert events 1-3
            for (int i = 1; i <= 3; i++) {
                Event event = createTestEvent(i);
                Publisher.publish(event, connection, TEST_TOPIC);
            }

            copyEventsToMessages(connection, 0);

            // Then insert events 4-6
            for (int i = 4; i <= 6; i++) {
                log.debug("Publishing event {}", i);
                Event event = createTestEvent(i);
                Publisher.publish(event, connection, TEST_TOPIC);
            }
            copyEventsToMessages(connection, 5);
            TestUtil.logEventsInTopicTable(connection, log, TEST_TOPIC);
            TestUtil.logEventsInMessagesTable(connection, log);

            // Initialize HWM to 0
            initializeHwm(connection, TEST_TOPIC, 0);

            // Check for gaps
            boolean hasGap = catchupService.hasSequenceGap(TEST_TOPIC, 0);

            // Verify a gap was found
            assertTrue(hasGap, "Should find a gap in the sequence");

            // Verify HWM was updated to the last event before the gap
            long newHwm = getCurrentHwm(connection, TEST_TOPIC);
            assertEquals(3, newHwm, "HWM should be updated to the last event before the gap");
        }
    }

    /**
     * Helper method to initialize HWM for a subscriber
     */
    private void initializeHwm(Connection connection, String topicName, long hwm) throws SQLException {
        String sql = "INSERT INTO postevent.contiguous_hwm (topic_name, hwm) " +
                "VALUES (?, ?) " +
                "ON CONFLICT (topic_name) DO UPDATE SET hwm = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topicName);
            stmt.setLong(2, hwm);
            stmt.setLong(3, hwm);

            stmt.executeUpdate();
        }
    }

    /**
     * Helper method to get current HWM for a subscriber
     */
    private long getCurrentHwm(Connection connection, String topicName) throws SQLException {
        String sql = "SELECT hwm FROM postevent.contiguous_hwm WHERE topic_name = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, topicName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("hwm");
                } else {
                    return 0;
                }
            }
        }
    }

    private int countMessagesInTable(Connection connection) throws Exception {
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