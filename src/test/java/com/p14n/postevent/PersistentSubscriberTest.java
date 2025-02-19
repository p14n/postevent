package com.p14n.postevent;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

class PersistentSubscriberTest {
    private EmbeddedPostgres pg;
    private Connection conn;
    private PersistentSubscriber persistentSubscriber;
    private MessageSubscriber<Event> mockSubscriber;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() throws Exception {
        pg = EmbeddedPostgres.builder().start();
        conn = pg.getPostgresDatabase().getConnection();

        // Add schema creation
        try (Connection conn = pg.getPostgresDatabase().getConnection()) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS postevent");
        }

        // Create test table
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS postevent.messages (
                        idn BIGSERIAL PRIMARY KEY,
                        id VARCHAR(255),
                        source VARCHAR(1024),
                        datacontenttype VARCHAR(255),
                        dataschema VARCHAR(255),
                        subject VARCHAR(255),
                        data BYTEA,
                        time TIMESTAMP WITH TIME ZONE
                    )
                    """);
        }

        mockSubscriber = Mockito.mock(MessageSubscriber.class);
        persistentSubscriber = new PersistentSubscriber(mockSubscriber, pg.getPostgresDatabase());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null)
            conn.close();
        if (pg != null)
            pg.close();
    }

    @Test
    void shouldPersistAndForwardEvent() throws Exception {
        // Create test event
        Event testEvent = Event.create(
                "test-123", "test-source", "test-type", "text/plain",
                "test-schema", "test-subject", "test-data".getBytes());

        // Test the subscriber
        persistentSubscriber.onMessage(testEvent);

        // Verify database persistence
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM postevent.messages")) {

            assertTrue(rs.next(), "No record found in database");
            assertEquals(testEvent.id(), rs.getString("id"));
            assertEquals(testEvent.source(), rs.getString("source"));
            assertEquals(testEvent.datacontenttype(), rs.getString("datacontenttype"));
            assertArrayEquals(testEvent.data(), rs.getBytes("data"));

            // Verify only one record exists
            assertFalse(rs.next(), "Multiple records found");
        }

        // Verify forwarding to subscriber
        verify(mockSubscriber).onMessage(testEvent);
    }
}