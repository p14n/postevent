package com.p14n.postevent.catchup.grpc;

import com.p14n.postevent.catchup.CatchupServerInterface;
import com.p14n.postevent.data.Event;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class CatchupGrpcIntegrationTest {

    private static final int PORT = 50051;
    private static final String TOPIC = "test-topic";
    private static final String HOST = "localhost";

    private CatchupServerInterface mockCatchupServer;
    private CatchupGrpcServer server;
    private CatchupGrpcClient client;

    @BeforeEach
    public void setUp() throws IOException {
        // Create mock server implementation
        mockCatchupServer = Mockito.mock(CatchupServerInterface.class);

        // Start the gRPC server
        server = new CatchupGrpcServer(PORT, mockCatchupServer);
        server.start();

        // Create the client
        client = new CatchupGrpcClient(HOST, PORT);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testFetchEvents() {
        // Setup test data
        long startAfter = 100L;
        long end = 200L;
        int maxResults = 10;

        // Create sample events
        Event event1 = createSampleEvent(101L);
        Event event2 = createSampleEvent(150L);
        List<Event> mockEvents = Arrays.asList(event1, event2);

        // Configure mock to return our sample events
        when(mockCatchupServer.fetchEvents(startAfter, end, maxResults, TOPIC))
                .thenReturn(mockEvents);

        // Call the client
        List<Event> fetchedEvents = client.fetchEvents(startAfter, end, maxResults, TOPIC);

        // Verify the mock was called with correct parameters
        verify(mockCatchupServer).fetchEvents(startAfter, end, maxResults, TOPIC);

        // Verify results
        assertEquals(2, fetchedEvents.size());

        // Verify event properties were correctly transferred
        Event fetchedEvent1 = fetchedEvents.get(0);
        assertEquals(event1.id(), fetchedEvent1.id());
        assertEquals(event1.source(), fetchedEvent1.source());
        assertEquals(event1.type(), fetchedEvent1.type());
        assertEquals(event1.datacontenttype(), fetchedEvent1.datacontenttype());
        assertEquals(event1.subject(), fetchedEvent1.subject());
        assertArrayEquals(event1.data(), fetchedEvent1.data());
        assertEquals(event1.idn(), fetchedEvent1.idn());

        // Time might have some precision differences due to conversion
        assertNotNull(fetchedEvent1.time());
    }

    @Test
    public void testFetchEventsWithError() {
        // Setup test data
        long startAfter = 100L;
        long end = 200L;
        int maxResults = 10;

        // Configure mock to throw exception
        when(mockCatchupServer.fetchEvents(startAfter, end, maxResults, TOPIC))
                .thenThrow(new RuntimeException("Test exception"));

        // Call the client and verify exception is propagated
        Exception exception = assertThrows(RuntimeException.class, () -> {
            client.fetchEvents(startAfter, end, maxResults, TOPIC);
        });

        assertTrue(exception.getMessage().contains("Failed to fetch events via gRPC"));
    }

    @Test
    public void testFetchEmptyEvents() {
        // Setup test data
        long startAfter = 100L;
        long end = 200L;
        int maxResults = 10;

        // Configure mock to return empty list
        when(mockCatchupServer.fetchEvents(startAfter, end, maxResults, TOPIC))
                .thenReturn(List.of());

        // Call the client
        List<Event> fetchedEvents = client.fetchEvents(startAfter, end, maxResults, TOPIC);

        // Verify results
        assertTrue(fetchedEvents.isEmpty());
    }

    private Event createSampleEvent(long idn) {
        String id = UUID.randomUUID().toString();
        String source = "test-source";
        String type = "test-type";
        String contentType = "application/json";
        String dataSchema = "schema1";
        String subject = "test-subject";
        byte[] data = "{\"key\":\"value\"}".getBytes();
        Instant time = Instant.now();

        return Event.create(
                id,
                source,
                type,
                contentType,
                dataSchema,
                subject,
                data,
                time,
                idn,
                "topic");
    }
}