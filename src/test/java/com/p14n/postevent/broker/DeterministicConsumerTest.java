package com.p14n.postevent.broker;

import com.p14n.postevent.ConsumerClient;
import com.p14n.postevent.ConsumerServer;
import com.p14n.postevent.Publisher;
import com.p14n.postevent.TestUtil;
import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.example.ExampleUtil;

import com.p14n.postevent.telemetry.DefaultTelemetryConfig;
import net.jqwik.api.*;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class DeterministicConsumerTest {
    private static final Logger logger = LoggerFactory.getLogger(DeterministicConsumerTest.class);
    private static final int PORT = 50052;
    private static final String TOPIC = "test_topic";


    @Property(tries = 10)
    void testDeterministicEventDelivery(@ForAll("randomSeeds") long seed) throws Exception {
        logger.atInfo().log("Testing with seed: {}", seed);
        Random random = new Random(seed);
        var executor = new TestAsyncExecutor();
        var telemetryConfig = new DefaultTelemetryConfig(DeterministicConsumerTest.class.getSimpleName());

        try (var pg = ExampleUtil.embeddedPostgres();) {

            var dataSource = pg.getPostgresDatabase();

            var config = new ConfigData(
                    TOPIC,
                    Set.of(TOPIC),
                    "127.0.0.1",
                    pg.getPort(),
                    "postgres",
                    "postgres",
                    "postgres");

            // Start server
            var server = new ConsumerServer(dataSource, config, executor,telemetryConfig);

            server.start(PORT);

            // Start client
            var client = new ConsumerClient(telemetryConfig,executor);
            client.start(Set.of(TOPIC), dataSource, "localhost", PORT);

            var receivedEventIdns = new CopyOnWriteArrayList<Long>();
            Set<String> receivedEventIds = ConcurrentHashMap.newKeySet();
            Set<String> publishedEventIds = ConcurrentHashMap.newKeySet();

            // Generate random number of events (1-100)
            int numberOfEvents = random.nextInt(100) + 1;
            var eventsLatch = new CountDownLatch(numberOfEvents);
            logger.atInfo().log("Testing with {} events", numberOfEvents);

            AtomicInteger failmod = new AtomicInteger(5);

            // Setup client subscriber
            client.subscribe(TOPIC, (TransactionalEvent event) -> {
                var eventIdn = event.event().idn();
                if (eventIdn % failmod.getAndIncrement() == 0) {
                    throw new RuntimeException("Fell over intentionally");
                }
                var eventId = event.event().id();
                receivedEventIds.add(eventId);
                logger.atInfo().log("Received event: {} {}", eventId, receivedEventIds.size());
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
                        logger.atInfo().log("Published event: {}", event.id());
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
                logger.atInfo().log("Tick {}: Received {} of {} events", tickCount, receivedEventIds.size(),
                        numberOfEvents);
            }
            Thread.sleep(2000);

            try (var connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                TestUtil.logEventsInTopicTable(connection, logger, TOPIC);
                TestUtil.logEventsInMessagesTable(connection, logger);
                TestUtil.logEventsInHwmTable(connection, logger);
            }

            logger.atInfo().log("Test completed in {} ticks", tickCount);
            logger.atInfo().log("Published events: {}", publishedEventIds.size());
            logger.atInfo().log("Received events: {}", receivedEventIds.size());
            logger.atInfo().log("Received event IDs: {}", receivedEventIdns);

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

            var telemetryConfig = new DefaultTelemetryConfig(DeterministicConsumerTest.class.getSimpleName());

            var dataSource = pg.getPostgresDatabase();

            var config = new ConfigData(
                    TOPIC,
                    Set.of(TOPIC),
                    "127.0.0.1",
                    pg.getPort(),
                    "postgres",
                    "postgres",
                    "postgres");

            var executor = new TestAsyncExecutor();

            // Start server
            var server = new ConsumerServer(dataSource, config, executor,telemetryConfig);

            server.start(PORT);

            // Start client
            var client = new ConsumerClient(telemetryConfig,executor);
            client.start(Set.of(TOPIC), dataSource, "localhost", PORT);

            logger.atInfo().log("Testing with seed: {}", seed);
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
            logger.atInfo().log("Testing with {} events", numberOfEvents);

            AtomicInteger failmod = new AtomicInteger(5);

            // Setup client subscriber
            client.subscribe(TOPIC, (TransactionalEvent event) -> {
                var eventIdn = event.event().idn();
                if (eventIdn % failmod.getAndIncrement() == 0) {
                    throw new RuntimeException("Fell over intentionally");
                }
                var eventId = event.event().id();
                receivedEventIds.add(eventId);
                logger.atInfo().log("Received event: {} {}", eventId, receivedEventIds.size());
                receivedEventIdns.get(event.event().subject()).add(eventIdn);
                eventsLatch.countDown();
            });

            // Schedule random event publications
            for (int i = 0; i < numberOfEvents; i++) {
                final int eventNumber = i;
                executor.submit(() -> {
                    try {
                        var event = TestUtil.createTestEvent(eventNumber, "subject" + (eventNumber % 3));
                        publishedEventIds.add(event.id());
                        Publisher.publish(event, dataSource, TOPIC);
                        logger.atInfo().log("Published event: {}", event.id());
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
                logger.atInfo().log("Tick {}: Received {} of {} events", tickCount, receivedEventIds.size(),
                        numberOfEvents);
            }
            Thread.sleep(2000);

            logger.atInfo().log("Test completed in {} ticks", tickCount);
            logger.atInfo().log("Published events: {}", publishedEventIds.size());
            logger.atInfo().log("Received events: {}", receivedEventIds.size());
            logger.atInfo().log("Received event IDs: {}", receivedEventIdns);

            // Assertions
            assertTrue(tickCount < maxTicks, "Test did not complete within maximum ticks(" + maxTicks + ")");
            assertEquals(numberOfEvents, publishedEventIds.size(),
                    "Not all events were published");
            assertEquals(numberOfEvents, receivedEventIds.size(),
                    "Not all events were received");
            assertTrue(receivedEventIds.containsAll(publishedEventIds),
                    "Not all published events were received");

            for (List<Long> receivedEventIdnsSubject : receivedEventIdns.values()) {
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

    @Property(tries = 5)
    void testMultipleTopicsWithDedicatedClients(@ForAll("randomSeeds") long seed) throws Exception {
        logger.atInfo().log("Testing with seed: {}", seed);
        Random random = new Random(seed);
        var executor = new TestAsyncExecutor();
        var telemetryConfig = new DefaultTelemetryConfig(DeterministicConsumerTest.class.getSimpleName());

        String topic1 = "test_topic_one";
        String topic2 = "test_topic_two";

        try (var pg = ExampleUtil.embeddedPostgres()) {
            var dataSource = pg.getPostgresDatabase();

            // Configure server with both topics
            var config = new ConfigData(
                    "server1",
                    Set.of(topic1, topic2),
                    "127.0.0.1",
                    pg.getPort(),
                    "postgres",
                    "postgres",
                    "postgres");

            // Start server
            var server = new ConsumerServer(dataSource, config, executor,telemetryConfig);
            server.start(PORT);

            // Start client for topic1
            var client1 = new ConsumerClient(telemetryConfig,executor);
            client1.start(Set.of(topic1), dataSource, "localhost", PORT);

            // Start client for topic2
            var client2 = new ConsumerClient(telemetryConfig,executor);
            client2.start(Set.of(topic2), dataSource, "localhost", PORT);

            // Track received events per topic
            Set<String> receivedEventIdsTopic1 = ConcurrentHashMap.newKeySet();
            Set<String> receivedEventIdsTopic2 = ConcurrentHashMap.newKeySet();
            Set<String> publishedEventIdsTopic1 = ConcurrentHashMap.newKeySet();
            Set<String> publishedEventIdsTopic2 = ConcurrentHashMap.newKeySet();

            // Generate random number of events per topic (1-50 each)
            int numberOfEventsTopic1 = random.nextInt(50) + 1;
            int numberOfEventsTopic2 = random.nextInt(50) + 1;
            int totalEvents = numberOfEventsTopic1 + numberOfEventsTopic2;

            var eventsLatch = new CountDownLatch(totalEvents);
            logger.atInfo().log("Testing with {} events for topic1 and {} events for topic2",
                    numberOfEventsTopic1, numberOfEventsTopic2);

            // Setup client1 subscriber
            client1.subscribe(topic1, (TransactionalEvent event) -> {
                var eventId = event.event().id();
                receivedEventIdsTopic1.add(eventId);
                logger.atInfo().log("Topic1 received event: {} {}", eventId, receivedEventIdsTopic1.size());
                eventsLatch.countDown();
            });

            // Setup client2 subscriber
            client2.subscribe(topic2, (TransactionalEvent event) -> {
                var eventId = event.event().id();
                receivedEventIdsTopic2.add(eventId);
                logger.atInfo().log("Topic2 received event: {} {}", eventId, receivedEventIdsTopic2.size());
                eventsLatch.countDown();
            });

            // Publish events to both topics
            for (int i = 0; i < numberOfEventsTopic1; i++) {
                final int eventNumber = i;
                executor.submit(() -> {
                    try {
                        var event = TestUtil.createTestEvent(eventNumber, "topic1_subject");
                        publishedEventIdsTopic1.add(event.id());
                        Publisher.publish(event, dataSource, topic1);
                        logger.atInfo().log("Published event to topic1: {}", event.id());
                        return event.id();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to publish event to topic1", e);
                    }
                });
            }

            for (int i = 0; i < numberOfEventsTopic2; i++) {
                final int eventNumber = i;
                executor.submit(() -> {
                    try {
                        var event = TestUtil.createTestEvent(eventNumber, "topic2_subject");
                        publishedEventIdsTopic2.add(event.id());
                        Publisher.publish(event, dataSource, topic2);
                        logger.atInfo().log("Published event to topic2: {}", event.id());
                        return event.id();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to publish event to topic2", e);
                    }
                });
            }

            // Calculate maximum ticks allowed
            int maxTicks = (totalEvents * 10) + 100;
            int tickCount = 0;

            // Tick until all events are received or max ticks reached
            while (tickCount < maxTicks &&
                    (receivedEventIdsTopic1.size() < numberOfEventsTopic1 ||
                            receivedEventIdsTopic2.size() < numberOfEventsTopic2)) {
                executor.tick(random, tickCount % 5 == 0);
                Thread.sleep(10);
                tickCount++;
                logger.atInfo().log("Tick {}: Topic1 received {}/{}, Topic2 received {}/{} events",
                        tickCount,
                        receivedEventIdsTopic1.size(), numberOfEventsTopic1,
                        receivedEventIdsTopic2.size(), numberOfEventsTopic2);
            }

            Thread.sleep(2000);

            try (var connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                TestUtil.logEventsInTopicTable(connection, logger, topic1);
                TestUtil.logEventsInTopicTable(connection, logger, topic2);
                TestUtil.logEventsInMessagesTable(connection, logger);
                TestUtil.logEventsInHwmTable(connection, logger);
            }
            Thread.sleep(100);
            // Assertions
            assertTrue(tickCount < maxTicks, "Test did not complete within maximum ticks(" + maxTicks + ")");

            // Topic1 assertions
            assertEquals(numberOfEventsTopic1, publishedEventIdsTopic1.size(),
                    "Not all events were published to topic1");
            assertEquals(numberOfEventsTopic1, receivedEventIdsTopic1.size(),
                    "Not all events were received on topic1");
            assertTrue(receivedEventIdsTopic1.containsAll(publishedEventIdsTopic1),
                    "Not all published events were received on topic1");
            assertTrue(Collections.disjoint(receivedEventIdsTopic1, publishedEventIdsTopic2),
                    "Topic1 client received events from topic2");

            // Topic2 assertions
            assertEquals(numberOfEventsTopic2, publishedEventIdsTopic2.size(),
                    "Not all events were published to topic2");
            assertEquals(numberOfEventsTopic2, receivedEventIdsTopic2.size(),
                    "Not all events were received on topic2");
            assertTrue(receivedEventIdsTopic2.containsAll(publishedEventIdsTopic2),
                    "Not all published events were received on topic2");
            assertTrue(Collections.disjoint(receivedEventIdsTopic2, publishedEventIdsTopic1),
                    "Topic2 client received events from topic1");

            close(server);
            close(client1);
            close(client2);
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
