package com.p14n.postevent;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.DatabaseSetup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import java.util.Random;

class PublisherTest {

    private EmbeddedPostgres postgres;
    private Connection conn;

    @BeforeEach
    void setUp() throws IOException, SQLException {
        postgres = EmbeddedPostgres.builder()
                .setServerConfig("postgresql.version", "16.2")
                .start();

        conn = postgres.getPostgresDatabase().getConnection();

        // Initialize and use DatabaseSetup
        var dbSetup = new DatabaseSetup(
                postgres.getJdbcUrl("postgres", "postgres"),
                "postgres",
                "postgres");
        dbSetup.createSchemaIfNotExists();
        dbSetup.createTableIfNotExists("test_topic");
    }

    @AfterEach
    void tearDown() throws IOException, SQLException {
        if (conn != null) {
            conn.close();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void shouldPublishEventSuccessfully() throws SQLException {
        // Given
        Event event = TestUtil.createTestEvent(0);

        // When
        Publisher.publish(event, conn, "test_topic");

        // Then
        verifyEventInDatabase(conn, event, "test_topic");
    }

    private void verifyEventInDatabase(Connection conn, Event event, String topic) throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM postevent." + topic)) {
            assertTrue(rs.next());
            assertEquals(event.id(), rs.getString("id"));
            assertEquals(event.source(), rs.getString("source"));
            assertEquals(event.type(), rs.getString("type"));
            assertEquals(event.datacontenttype(), rs.getString("datacontenttype"));
            assertEquals(event.dataschema(), rs.getString("dataschema"));
            assertEquals(event.subject(), rs.getString("subject"));
        }
    }

    @Test
    void shouldThrowExceptionForEmptyTopic() {
        Event event = TestUtil.createTestEvent(0);

        assertThrows(IllegalArgumentException.class, () -> Publisher.publish(event, conn, ""));
    }

    @Test
    void shouldThrowExceptionForNullTopic() {
        Event event = TestUtil.createTestEvent(0);

        assertThrows(IllegalArgumentException.class, () -> Publisher.publish(event, conn, null));
    }

    @Test
    void shouldHandleNullFields() throws SQLException {
        // Given
        Event event = TestUtil.createTestEvent(0);

        // When
        Publisher.publish(event, conn, "test_topic");

        // Then
        verifyEventInDatabase(conn, event, "test_topic");
    }

    @Test
    void shouldHandleLargePayload() throws SQLException {
        // Given
        byte[] largeData = new byte[10 * 1024 * 1024]; // 10MB payload
        new Random().nextBytes(largeData);

        Event event = Event.create(
                "test-id",
                "test-source",
                "test-type",
                "application/json",
                "test-schema",
                "test-subject",
                largeData,
                null);

        // When
        Publisher.publish(event, conn, "test_topic");

        // Then
        verifyEventInDatabase(conn, event, "test_topic");
    }

    @Test
    void shouldHandleClosedConnection() throws SQLException {
        // Given
        Event event = Event.create(
                "test-id",
                "test-source",
                "test-type",
                "application/json",
                "test-schema",
                "test-subject",
                "test-data".getBytes(),
                null);
        conn.close();

        // When/Then
        assertThrows(SQLException.class, () -> Publisher.publish(event, conn, "test_topic"));
    }

    @Test
    void shouldHandleNonExistentTable() throws SQLException {
        // Given
        Event event = Event.create(
                "test-id",
                "test-source",
                "test-type",
                "application/json",
                "test-schema",
                "test-subject",
                "test-data".getBytes(),
                null);

        // When/Then
        assertThrows(SQLException.class, () -> Publisher.publish(event, conn, "non_existent_topic"));
    }

    @Test
    void shouldHandleConcurrentAccess() throws Exception {
        // Given
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        // When
        for (int i = 0; i < threadCount; i++) {
            final String id = "test-id-" + i;
            futures.add(executor.submit(() -> {
                try {
                    Event event = Event.create(
                            id,
                            "test-source",
                            "test-type",
                            "application/json",
                            "test-schema",
                            "test-subject",
                            "test-data".getBytes(),
                            null);
                    Publisher.publish(event, conn, "test_topic");
                    latch.countDown();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Then
        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM postevent.test_topic")) {
            assertTrue(rs.next());
            assertEquals(threadCount, rs.getInt("count"));
        }
    }
}
