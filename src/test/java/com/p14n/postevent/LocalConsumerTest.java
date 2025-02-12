package com.p14n.postevent;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class LocalConsumerTest {
    private EmbeddedPostgres pg;
    private Connection conn;
    private LocalConsumer localConsumer;
    private DefaultMessageBroker<Event> broker;
    private Publisher publisher;

    @BeforeEach
    void setUp() throws Exception {
        pg = EmbeddedPostgres.builder()
                .setServerConfig("wal_level", "logical")
                .setServerConfig("max_wal_senders", "3")
                .start();

        conn = pg.getPostgresDatabase().getConnection();
        var setup = new DatabaseSetup(pg.getJdbcUrl("postgres", "postgres"), "postgres", "postgres");
        setup.createSchemaIfNotExists();
        setup.createTableIfNotExists("test");

        broker = new DefaultMessageBroker<>();
        PostEventConfig config = new ConfigData(
                "test",
                "test",
                "localhost",
                pg.getPort(),
                "postgres",
                "postgres",
                "postgres",
                null);

        publisher = new Publisher();
        localConsumer = new LocalConsumer(config, broker);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (localConsumer != null) {
            localConsumer.stop();
        }
        if (conn != null) {
            conn.close();
        }
        if (pg != null) {
            pg.close();
        }
    }

    @Test
    void shouldReceivePublishedEvent() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Event> receivedEvent = new AtomicReference<>();

        // Setup consumer
        broker.subscribe(new MessageSubscriber<Event>() {
            @Override
            public void onMessage(Event event) {
                receivedEvent.set(event);
                latch.countDown();
            }

            @Override
            public void onError(Throwable error) {
                throw new RuntimeException(error);
            }
        });

        localConsumer.start();

        // Create and publish test event
        Event testEvent = new Event(
                "test-id", "test-source", "test-type",
                "text/plain", "test-schema", "test-subject",
                "test-data".getBytes());

        publisher.publish(testEvent, conn, "test");

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Did not receive event within timeout");

        Event actual = receivedEvent.get();
        assertNotNull(actual, "No event received");
        assertEquals(testEvent.id(), actual.id());
        assertEquals(testEvent.source(), actual.source());
        assertEquals(testEvent.type(), actual.type());
        assertArrayEquals(testEvent.data(), actual.data());
    }
}