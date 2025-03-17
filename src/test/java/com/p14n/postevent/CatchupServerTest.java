package com.p14n.postevent;

import com.p14n.postevent.catchup.CatchupServer;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.DatabaseSetup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CatchupServerTest {

    private static final String TEST_TOPIC = "test_topic";

    private EmbeddedPostgres pg;
    private CatchupServer catchupServer;

    @BeforeEach
    public void setup() throws Exception {
        // Setup embedded PostgreSQL
        pg = EmbeddedPostgres.start();
        String jdbcUrl = pg.getJdbcUrl("postgres", "postgres");

        // Setup database
        DatabaseSetup setup = new DatabaseSetup(jdbcUrl, "postgres", "postgres");
        setup.createSchemaIfNotExists();
        setup.createTableIfNotExists(TEST_TOPIC);

        // Create catchup server
        catchupServer = new CatchupServer(pg.getPostgresDatabase());
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (pg != null) {
            pg.close();
        }
    }

    @Test
    public void testFetchEvents() throws Exception {
        // Publish some test events
        try (Connection connection = DriverManager.getConnection(pg.getJdbcUrl("postgres", "postgres"), "postgres",
                "postgres")) {
            for (int i = 0; i < 10; i++) {
                Event event = TestUtil.createTestEvent(i);
                Publisher.publish(event, connection, TEST_TOPIC);
            }
        }
        // Fetch events
        List<Event> events = catchupServer.fetchEvents(1, 5, 10,TEST_TOPIC);

        // Verify results
        assertEquals(4, events.size());
    }

    @Test
    public void testMaxResultsIsRespected() throws Exception {
        // Publish 20 test events
        try (Connection connection = DriverManager.getConnection(pg.getJdbcUrl("postgres", "postgres"), "postgres",
                "postgres")) {
            for (int i = 0; i < 20; i++) {
                Event event = TestUtil.createTestEvent(i);
                Publisher.publish(event, connection, TEST_TOPIC);
            }
        }

        // Test with different maxResults values
        List<Event> events1 = catchupServer.fetchEvents(1, 20, 5,TEST_TOPIC);
        assertEquals(5, events1.size());

        List<Event> events2 = catchupServer.fetchEvents(1, 20, 10,TEST_TOPIC);
        assertEquals(10, events2.size());

        List<Event> events3 = catchupServer.fetchEvents(1, 20, 15,TEST_TOPIC);
        assertEquals(15, events3.size());

        // When maxResults is greater than available events
        List<Event> events4 = catchupServer.fetchEvents(10, 20, 20,TEST_TOPIC);
        assertEquals(10, events4.size());
    }

    @Test
    public void testMaxResultsLimitsLargeRange() throws Exception {
        // Publish 50 test events
        try (Connection connection = DriverManager.getConnection(pg.getJdbcUrl("postgres", "postgres"), "postgres",
                "postgres")) {
            for (int i = 0; i < 50; i++) {
                Event event = TestUtil.createTestEvent(i);
                Publisher.publish(event, connection, TEST_TOPIC);
            }
        }

        // Request a large range but limit with maxResults
        List<Event> events = catchupServer.fetchEvents(0, 50, 25,TEST_TOPIC);

        // Verify maxResults is respected
        assertEquals(25, events.size());

        // Verify we got the first 25 events
        byte[] firstEventData = events.get(0).data();
        String firstEventJson = new String(firstEventData);
        assertTrue(firstEventJson.contains("\"value\":0"));

        byte[] lastEventData = events.get(24).data();
        String lastEventJson = new String(lastEventData);
        assertTrue(lastEventJson.contains("\"value\":24"));
    }

    @Test
    public void testInvalidParameters() {
        // Test invalid start/end
        assertThrows(IllegalArgumentException.class, () -> catchupServer.fetchEvents(10, 5, 10,TEST_TOPIC));

        // Test invalid maxResults
        assertThrows(IllegalArgumentException.class, () -> catchupServer.fetchEvents(1, 10, 0,TEST_TOPIC));
    }
}