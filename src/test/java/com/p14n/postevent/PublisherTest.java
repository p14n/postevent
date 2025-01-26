package com.p14n.postevent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class PublisherTest {

    private EmbeddedPostgres postgres;
    private Connection conn;
    private Publisher publisher;
    private DatabaseSetup dbSetup;

    @BeforeEach
    void setUp() throws IOException, SQLException {
        postgres = EmbeddedPostgres.builder()
                .setServerConfig("postgresql.version", "16.2")
                .start();

        conn = postgres.getPostgresDatabase().getConnection();
        publisher = new Publisher();

        // Initialize and use DatabaseSetup
        dbSetup = new DatabaseSetup(
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
        Event event = new Event(
                "test-id",
                "test-source",
                "test-type",
                "application/json",
                "test-schema",
                "test-subject",
                "test-data".getBytes());

        // When
        publisher.publish(event, conn, "test_topic");

        // Then
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM postevent.test_topic")) {

            assertTrue(rs.next());
            assertEquals("test-id", rs.getString("id"));
            assertEquals("test-source", rs.getString("source"));
            assertEquals("test-type", rs.getString("type"));
            assertEquals("application/json", rs.getString("datacontenttype"));
            assertEquals("test-schema", rs.getString("dataschema"));
            assertEquals("test-subject", rs.getString("subject"));
            assertArrayEquals("test-data".getBytes(), rs.getBytes("data"));
            assertFalse(rs.next());
        }
    }

    @Test
    void shouldThrowExceptionForEmptyTopic() {
        Event event = new Event("id", "source", "type", null, null, null, null);

        assertThrows(IllegalArgumentException.class, () -> publisher.publish(event, conn, ""));
    }

    @Test
    void shouldThrowExceptionForNullTopic() {
        Event event = new Event("id", "source", "type", null, null, null, null);

        assertThrows(IllegalArgumentException.class, () -> publisher.publish(event, conn, null));
    }
}