package com.p14n.postevent.broker;

import com.p14n.postevent.ConsumerClient;
import com.p14n.postevent.ConsumerServer;
import com.p14n.postevent.Publisher;
import com.p14n.postevent.TestUtil;
import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.example.ExampleUtil;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import net.jqwik.api.*;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import javax.sql.DataSource;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

class DeterministicConsumerTest {

    private static final Logger LOGGER = Logger.getLogger(DeterministicConsumerTest.class.getName());
    private static final int PORT = 50052;
    private static final String TOPIC = "test_topic";

    @Property(tries = 2)
    void testDeterministicEventDelivery(@ForAll("randomSeeds") long seed) throws Exception {

        try (var pg = ExampleUtil.embeddedPostgres();) {

            var dataSource = pg.getPostgresDatabase();

            var config = new ConfigData(
                    TOPIC,
                    TOPIC,
                    "127.0.0.1",
                    pg.getPort(),
                    "postgres",
                    "postgres",
                    "postgres");

            var executor = new TestAsyncExecutor();

            // Start server
            var server = new ConsumerServer(dataSource, config, executor);

            server.start(PORT);

            // Start client
            var client = new ConsumerClient(TOPIC, executor);
            client.start(dataSource, "localhost", PORT);

            LOGGER.info("Testing with seed: " + seed );
            Random random = new Random(seed);

            var receivedEventIdns = new CopyOnWriteArrayList<Long>();
            Set<String> receivedEventIds = ConcurrentHashMap.newKeySet();
            Set<String> publishedEventIds = ConcurrentHashMap.newKeySet();

            // Generate random number of events (1-100)
            int numberOfEvents = random.nextInt(100) + 1;
            var eventsLatch = new CountDownLatch(numberOfEvents);
            LOGGER.info("Testing with " + numberOfEvents + " events");

            AtomicInteger failmod = new AtomicInteger(5);

            // Setup client subscriber
            client.subscribe((TransactionalEvent event) -> {
                var eventIdn = event.event().idn();
                if(eventIdn % failmod.getAndIncrement() == 0) {
                    throw new RuntimeException("Fell over intentionally");
                }
                var eventId = event.event().id();
                receivedEventIds.add(eventId);
                LOGGER.info("Received event: " + eventId + " " + receivedEventIds.size());
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
            //
            // Calculate maximum ticks allowed
            int maxTicks = (numberOfEvents * 10) + 100;
            int tickCount = 0;

            // Tick until all events are received or max ticks reached
            while (tickCount < maxTicks && receivedEventIds.size() < numberOfEvents) {
                executor.tick(random, tickCount % 5 == 0);
                Thread.sleep(10);
                tickCount++;
                LOGGER.info("Tick " + tickCount + ": Received " + receivedEventIds.size() + " of " + numberOfEvents
                        + " events");
            }
            Thread.sleep(2000);

            LOGGER.info("Test completed in " + tickCount + " ticks");
            LOGGER.info("Published events: " + publishedEventIds.size());
            LOGGER.info("Received events: " + receivedEventIds.size());
            LOGGER.info("Received event IDs: " + receivedEventIdns);

            // Assertions
            assertTrue(tickCount < maxTicks, "Test did not complete within maximum ticks(" + maxTicks + ")");
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
            close(server);
            close(client);
            close(executor);
        }
    }
    @Property(tries = 2)
    void testDeterministicEventDeliveryBySubject(@ForAll("randomSeeds") long seed) throws Exception {

        try (var pg = ExampleUtil.embeddedPostgres();) {

            var dataSource = pg.getPostgresDatabase();

            var config = new ConfigData(
                    TOPIC,
                    TOPIC,
                    "127.0.0.1",
                    pg.getPort(),
                    "postgres",
                    "postgres",
                    "postgres");

            var executor = new TestAsyncExecutor();

            // Start server
            var server = new ConsumerServer(dataSource, config, executor);

            server.start(PORT);

            // Start client
            var client = new ConsumerClient(TOPIC, executor);
            client.start(dataSource, "localhost", PORT);

            LOGGER.info("Testing with seed: " + seed );
            Random random = new Random(seed);

            var receivedEventIdns = new ConcurrentHashMap<String, List<Long>>();
            receivedEventIdns.putIfAbsent("subject0", new CopyOnWriteArrayList<Long>());
            receivedEventIdns.putIfAbsent("subject1", new CopyOnWriteArrayList<Long>());
            receivedEventIdns.putIfAbsent("subject2", new CopyOnWriteArrayList<Long>());
            Set<String> receivedEventIds = ConcurrentHashMap.newKeySet();
            Set<String> publishedEventIds = ConcurrentHashMap.newKeySet();

            // Generate random number of events (1-100)
            int numberOfEvents = random.nextInt(100) + 1;
            var eventsLatch = new CountDownLatch(numberOfEvents);
            LOGGER.info("Testing with " + numberOfEvents + " events");

            AtomicInteger failmod = new AtomicInteger(5);

            // Setup client subscriber
            client.subscribe((TransactionalEvent event) -> {
                var eventIdn = event.event().idn();
                if(eventIdn % failmod.getAndIncrement() == 0) {
                    throw new RuntimeException("Fell over intentionally");
                }
                var eventId = event.event().id();
                receivedEventIds.add(eventId);
                LOGGER.info("Received event: " + eventId + " " + receivedEventIds.size());
                receivedEventIdns.get(event.event().subject()).add(eventIdn);
                eventsLatch.countDown();
            });

            // Schedule random event publications
            for (int i = 0; i < numberOfEvents; i++) {
                final int eventNumber = i;
                executor.submit(() -> {
                    try {
                        var event = TestUtil.createTestEvent(eventNumber,"subject" + (eventNumber % 3));
                        publishedEventIds.add(event.id());
                        Publisher.publish(event, dataSource, TOPIC);
                        LOGGER.info("Published event: " + event.id());
                        return event.id();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to publish event", e);
                    }
                });
            }
            //
            // Calculate maximum ticks allowed
            int maxTicks = (numberOfEvents * 10) + 100;
            int tickCount = 0;

            // Tick until all events are received or max ticks reached
            while (tickCount < maxTicks && receivedEventIds.size() < numberOfEvents) {
                executor.tick(random, tickCount % 5 == 0);
                Thread.sleep(10);
                tickCount++;
                LOGGER.info("Tick " + tickCount + ": Received " + receivedEventIds.size() + " of " + numberOfEvents
                        + " events");
            }
            Thread.sleep(2000);

            LOGGER.info("Test completed in " + tickCount + " ticks");
            LOGGER.info("Published events: " + publishedEventIds.size());
            LOGGER.info("Received events: " + receivedEventIds.size());
            LOGGER.info("Received event IDs: " + receivedEventIdns);

            // Assertions
            assertTrue(tickCount < maxTicks, "Test did not complete within maximum ticks(" + maxTicks + ")");
            assertEquals(numberOfEvents, publishedEventIds.size(),
                    "Not all events were published");
            assertEquals(numberOfEvents, receivedEventIds.size(),
                    "Not all events were received");
            assertTrue(receivedEventIds.containsAll(publishedEventIds),
                    "Not all published events were received");

            for(List<Long> receivedEventIdnsSubject: receivedEventIdns.values()){
                for (int i = 0; i < receivedEventIdnsSubject.size() - 1; i++) {
                    assertTrue(receivedEventIdnsSubject.get(i) < receivedEventIdnsSubject.get(i + 1),
                            "Events were not received in order");
                }
            }
            close(server);
            close(client);
            close(executor);
        }
    }

    private void close(AutoCloseable c) {
        try {
            if (c != null)
                c.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Provide
    Arbitrary<Long> randomSeeds() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE);
    }
}
