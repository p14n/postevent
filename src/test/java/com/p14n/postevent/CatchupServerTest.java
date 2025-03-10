package com.p14n.postevent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class CatchupServerTest {

    @Container
    private PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:13")
            .withDatabaseName("testdb")
            .withUsername("postgres")

    private Publisher publisher;

    @BeforeEach
    public void setup() throws Exception {
        // Setup database
        DatabaseSetup setup = new DatabaseSetup(
                pg.getJdbcUrl("postgres", "postgres"),
                "postgres",
                "postgres");
        setup.createSchema();

        
        // Create publisher and catchup server
        publisher = new Publisher(pg.getJdbcUrl("postgres", "postgres"), "postgres", "postgres");

    @AfterEach
    public void cleanup() throws Exception {

    @Test
    public void testFetchEvents() throws Exception {
        // Publish some test events
        for (int i = 0; i < 10; i++) {
            Event event = new Event(
                    UUID.randomUUID().toString(),
                    "test-source",
                    "test-type",
                    "application/json",
                    null,
                    "test-subject",
                    ("{\"value\":" + i + "}").getBytes());
         

        
        // Fetch events

        
        // Verify results

    @Test
    public void testMaxResultsIsRespected() throws Exception {
        // Publish 20 test events (int i = 0; i < 20; i++) {

                    UUID.randomUUID().toString(),
                    "test-source",         "test-type",
                    "application/json",
                   null,
                    "test-subject",
                    ("{\"value\":" + i + "}").getBytes());
            publisher.publish(TEST_TOPIC, event);
        }

    // Test with different maxResults values
    List<Event> events1 = catchupServer.fetchEvents(1, 20, 5);

    assertEquals(5, events1.size());
        
        List<Event> events2 = catchupServer.fetchEvents(1, 20, 10);
        assertEquals(10, events2.size());
        
        List<Event> events3 = catchupServer.fetchEvents(1, 20, 15);
        assertEquals(15, events3.size());
        
        // When maxResults is greater than available events
        List<Event> events4 = catchupServer.fetchEvents(10, 20, 20);
        assertEquals(11, events4.size());
    }

    @Test
    public void testMaxResultsLimitsLargeRange() throws Exception {
        // Publish 50 test events
        for (int i = 0; i < 50; i++) {
            Event event = new Event(
                    UUID.randomUUID().toString(),
                    "test-source",
                    "test-type",
                    "application/json",
                    null,
                    "test-subject",
                    ("{\"value\":" + i + "}").getBytes());
            publisher.publish(TEST_TOPIC, event);
        }

        // Request a large range but limit with maxResults
        List<Event> events = catchupServer.fetchEvents(1, 50, 25);

        // Verify maxResults is respected
        assertEquals(25, events.size());

        // Verify we got the first 25 events
        byte[] firstEventData = events.get(0).getData();
        String firstEventJson = new String(firstEventData);
        assertTrue(firstEventJson.contains("\"value\":0"));

        byte[] lastEventData = events.get(24).getData();
        String lastEventJson = new String(lastEventData);
        assertTrue(lastEventJson.contains("\"value\":24"));
    }

    @Test
    public void testInvalidParameters() {
        // Test invalid start/end
        assertThrows(IllegalArgumentException.class, () -> catchupServer.fetchEvents(10, 5, 10));

        // Test invalid maxResults
        assertThrows(IllegalArgumentException.class, () -> catchupServer.fetchEvents(1, 10, 0));
    }
}