package com.p14n.postevent.broker.grpc;

import com.p14n.postevent.broker.DefaultExecutor;
import com.p14n.postevent.broker.DefaultMessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.data.Event;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opentelemetry.api.OpenTelemetry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class MessageBrokerGrpcIntegrationTest {

    private static final int PORT = 50052;
    private static final String HOST = "localhost";
    private static final String TOPIC = "topic";

    private Server server;
    private MessageBrokerGrpcClient client;
    private TestMessageBroker messageBroker;
    private static final Logger logger = LoggerFactory.getLogger(MessageBrokerGrpcIntegrationTest.class);

    @BeforeEach
    public void setUp() throws IOException {

        var ot = OpenTelemetry.noop();
        // Create the message broker
        messageBroker = new TestMessageBroker(ot);

        // Create and start the gRPC server
        MessageBrokerGrpcServer grpcServer = new MessageBrokerGrpcServer(messageBroker);
        server = ServerBuilder.forPort(PORT)
                .addService(grpcServer)
                // .keepAliveTime(1, TimeUnit.HOURS)
                // .keepAliveTimeout(30, TimeUnit.SECONDS)
                .permitKeepAliveTime(1, TimeUnit.HOURS)
                .permitKeepAliveWithoutCalls(true)
                .build()
                .start();

        // Create the client
        client = new MessageBrokerGrpcClient(new DefaultExecutor(2),ot, HOST, PORT);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSubscribeToEvents() throws Exception {
        // Setup test
        int eventCount = 5;
        CountDownLatch eventsReceived = new CountDownLatch(eventCount);
        List<Event> receivedEvents = new ArrayList<>();
        AtomicBoolean errorOccurred = new AtomicBoolean(false);

        // Add a subscriber to the client to collect events
        client.subscribe(TOPIC, new MessageSubscriber<Event>() {
            @Override
            public void onMessage(Event event) {
                receivedEvents.add(event);
                eventsReceived.countDown();
            }

            @Override
            public void onError(Throwable error) {
                errorOccurred.set(true);
                eventsReceived.countDown(); // Ensure latch completes even on error
            }
        });
        Thread.sleep(200);

        // Publish events to the broker
        for (int i = 0; i < eventCount; i++) {
            Event event = createSampleEvent(i + 1);

            logger.info("Publishing event: " + i + " " + event.id());
            messageBroker.publish(TOPIC, event);
            logger.info("Published event: " + event.id());
            // Small delay to avoid overwhelming the stream
            Thread.sleep(50);
        }

        // Wait for all events to be received
        boolean allEventsReceived = eventsReceived.await(5, TimeUnit.SECONDS);

        // Assertions
        assertFalse(errorOccurred.get(), "An error occurred during event processing");
        assertTrue(allEventsReceived, "Not all events were received within timeout");
        assertEquals(eventCount, receivedEvents.size(), "Incorrect number of events received");

        // Verify event data
        for (int i = 0; i < eventCount; i++) {
            Event original = messageBroker.getPublishedEvents().get(i);
            Event received = receivedEvents.get(i);

            assertEquals(original.id(), received.id());
            assertEquals(original.source(), received.source());
            assertEquals(original.type(), received.type());
            assertEquals(original.datacontenttype(), received.datacontenttype());
            assertEquals(original.dataschema(), received.dataschema());
            assertEquals(original.subject(), received.subject());
            assertArrayEquals(original.data(), received.data());
            assertEquals(original.idn(), received.idn());
            // Time might have some precision differences due to conversion
            assertNotNull(received.time());
        }
    }

    @Test
    public void testUnsubscribe() throws Exception {
        // Setup test
        CountDownLatch eventReceived = new CountDownLatch(1);
        AtomicInteger eventReceivedCount = new AtomicInteger(0);

        // Add a subscriber to the client
        MessageSubscriber<Event> subscriber = new MessageSubscriber<Event>() {
            @Override
            public void onMessage(Event event) {
                eventReceived.countDown();
                eventReceivedCount.incrementAndGet();
                logger.info("Event received in test: " + event.id());
            }

            @Override
            public void onError(Throwable error) {
                // Ignore errors for this test
            }
        };

        client.subscribe(TOPIC, subscriber);
        Thread.sleep(200);

        // Publish an event
        Event event1 = createSampleEvent(1);
        messageBroker.publish(TOPIC, event1);

        // Wait for the event to be received
        logger.info("Waiting for event to be received");
        boolean received = eventReceived.await(5, TimeUnit.SECONDS);
        assertTrue(received, "Event was not received");

        // Unsubscribe
        logger.info("Unsubscribing from events");
        client.unsubscribe(TOPIC, subscriber);
        logger.info("Unsubscribed from events");

        // Publish another event
        Event event2 = createSampleEvent(2);
        messageBroker.publish(TOPIC, event2);

        // Wait a bit to see if the event is received
        logger.info("Waiting for secondevent to be received");
        Thread.sleep(1000);

        // Verify no events were received after unsubscribe
        logger.info("Checking if event was received");
        assertFalse(eventReceivedCount.get() > 1, "Event was received after unsubscribe");
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
                TOPIC,
                null);
    }

    /**
     * Test implementation of MessageBroker that allows controlling event flow
     */
    private static class TestMessageBroker extends DefaultMessageBroker<Event, Event> {
        private final List<Event> publishedEvents = new ArrayList<>();

        public TestMessageBroker(OpenTelemetry ot) {
            super(ot,"test");
        }

        @Override
        public void publish(String topic, Event event) {
            publishedEvents.add(event);
            logger.info("Published event in test broker: " + event.id());
            super.publish(topic, event);
        }

        @Override
        public Event convert(Event m) {
            return m;
        }

        public List<Event> getPublishedEvents() {
            return publishedEvents;
        }

    }
}