package com.p14n.postevent.broker;

import com.p14n.postevent.ConsumerClient;
import com.p14n.postevent.ConsumerServer;
import com.p14n.postevent.Publisher;
import com.p14n.postevent.TestUtil;
import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.example.ExampleUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

class DeterministicConsumerTest {
    private static final Logger LOGGER = Logger.getLogger(DeterministicConsumerTest.class.getName());
    private static final int PORT = 50052;
    private static final String TOPIC = "test_topic";

    private Random random;
    private TestAsyncExecutor executor;
    private ConsumerServer server;
    private ConsumerClient client;
    private DataSource dataSource;
    private ConfigData config;
    private CountDownLatch eventsLatch;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize with a fixed seed for reproducibility
        random = new Random(42);
        executor = new TestAsyncExecutor(random);

        // Setup PostgreSQL
        var pg = ExampleUtil.embeddedPostgres();
        dataSource = pg.getPostgresDatabase();

        config = new ConfigData(
                TOPIC,
                TOPIC,
                "127.0.0.1",
                pg.getPort(),
                "postgres",
                "postgres",
                "postgres");

        // Start server
        server = new ConsumerServer(dataSource, config, executor);
        server.start(PORT);

        // Start client
        client = new ConsumerClient(TOPIC, executor);
        client.start(dataSource, "localhost", PORT);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
        if (executor != null) {
            executor.close();
        }
    }

    @Test
    void testDeterministicEventDelivery() throws Exception {

        var receivedEventIdns = new CopyOnWriteArrayList<Long>();
        Set<String> receivedEventIds = ConcurrentHashMap.newKeySet();
        Set<String> publishedEventIds = ConcurrentHashMap.newKeySet();

        // Generate random number of events (1-100)
        int numberOfEvents = random.nextInt(100) + 1;
        eventsLatch = new CountDownLatch(numberOfEvents);
        LOGGER.info("Testing with " + numberOfEvents + " events");

        // Setup client subscriber
        client.subscribe((TransactionalEvent event) -> {
            var eventIdn = event.event().idn();
            var eventId = event.event().id();
            LOGGER.info("Received event: " + eventId);
            receivedEventIds.add(eventId);
            receivedEventIdns.add(eventIdn);
            eventsLatch.countDown();
        });

        // Schedule random event publications
        for (int i = 0; i < numberOfEvents; i++) {
            final int eventNumber = i;
            executor.submit(() -> {
                try {
                    var event = TestUtil.createTestEvent(eventNumber);
                    publishedEventIds.add(event.id());
                    Publisher.publish(event, dataSource, TOPIC);
                    LOGGER.info("Published event: " + event.id());
                    return event.id();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to publish event", e);
                }
            });
        }

        // Calculate maximum ticks allowed
        int maxTicks = numberOfEvents * 10;
        int tickCount = 0;

        // Tick until all events are received or max ticks reached
        while (tickCount < maxTicks && receivedEventIds.size() < numberOfEvents) {
            executor.tick();
            Thread.sleep(10);
            tickCount++;
            LOGGER.info("Tick " + tickCount + ": Received " + receivedEventIds.size() + " of " + numberOfEvents
                    + " events");
        }

        LOGGER.info("Test completed in " + tickCount + " ticks");
        LOGGER.info("Published events: " + publishedEventIds.size());
        LOGGER.info("Received events: " + receivedEventIds.size());
        LOGGER.info("Received event IDs: " + receivedEventIdns);

        // Assertions
        // assertTrue(tickCount < maxTicks, "Test did not complete within maximum
        // ticks(" + maxTicks + ")");
        assertEquals(numberOfEvents, publishedEventIds.size(),
                "Not all events were published");
        assertEquals(numberOfEvents, receivedEventIds.size(),
                "Not all events were received");
        assertTrue(receivedEventIds.containsAll(publishedEventIds),
                "Not all published events were received");
        for (int i = 0; i < receivedEventIdns.size() - 1; i++) {
            assertTrue(receivedEventIdns.get(i) < receivedEventIdns.get(i + 1),
                    "Events were not received in order");
        }
    }
}
