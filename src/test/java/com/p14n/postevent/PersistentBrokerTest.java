package com.p14n.postevent;

import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.catchup.PersistentBroker;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.DatabaseSetup;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

class PersistentBrokerTest {
    private EmbeddedPostgres pg;
    private Connection conn;
    private PersistentBroker persistentBroker;
    private MessageBroker<Event, Event> mockSubscriber;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() throws Exception {
        pg = EmbeddedPostgres.builder().start();

        // Create schema and messages table
        new DatabaseSetup(pg.getJdbcUrl("postgres", "postgres"), "postgres", "postgres")
                .createSchemaIfNotExists()
                .createMessagesTableIfNotExists()
                .createContiguousHwmTableIfNotExists();

        conn = pg.getPostgresDatabase().getConnection();
        conn.setAutoCommit(false);
        mockSubscriber = Mockito.mock(MessageBroker.class);
        persistentBroker = new PersistentBroker(mockSubscriber, pg.getPostgresDatabase(),
                new SystemEventBroker());
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
                "test-schema", "test-subject", "test-data".getBytes(), Instant.now(), 1L, "topic");

        try (Statement stmt = conn.createStatement();) {
            stmt.executeUpdate("insert into postevent.contiguous_hwm (topic_name, hwm) values ('topic',0)");
            conn.commit();
        }

        // Test the subscriber
        persistentBroker.publish("topic", testEvent);

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
        verify(mockSubscriber).publish("topic", testEvent);
    }
}