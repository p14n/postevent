package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

class PostgresDebeziumConnectorTest {
    private Debezium engine;
    private EmbeddedPostgres pg;
    private Connection conn;

    @BeforeEach
    void setUp() throws IOException, SQLException {

        pg = EmbeddedPostgres.builder()
                .setServerConfig("wal_level", "logical")
                .setServerConfig("max_wal_senders", "3")
                .start();
        conn = pg.getPostgresDatabase().getConnection();
        var jdbcUrl = pg.getJdbcUrl("postgres", "postgres");
        var databaseSetup = new DatabaseSetup(jdbcUrl, "postgres", "postgres");
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createTableIfNotExists("test");
        engine = new Debezium();
    }

    @AfterEach
    void tearDown() throws IOException, SQLException {
        if (engine != null) {
            engine.stop();
        }
        if (conn != null) {
            conn.close();
        }
        if (pg != null) {
            pg.close();
        }
    }

    @Test
    void engineCreatedSuccessfully() {
        assertNotNull(engine);
    }

    @Test
    void throwsExceptionWhenStartedWithoutConsumer() {
        assertThrows(IllegalStateException.class, () -> engine.start(null, null));
    }

    @Test
    void engineStartsWithConsumer() throws IOException, InterruptedException {
        AtomicBoolean consumerCalled = new AtomicBoolean(false);
        ConfigData cfg = new ConfigData("test", "test", "localhost", pg.getPort(), "postgres", "postgres",
                "postgres", null);
        engine.start(cfg, (ChangeEvent<String, String> event) -> {
            consumerCalled.set(true);
        });
        engine.stop();
        // Note: In a real test, you would need to trigger a database change
        // and verify that the consumer is called
    }

    @Test
    void engineReceivesMessage() throws SQLException, IOException, InterruptedException {

        ConfigData cfg = new ConfigData("test", "test", "localhost", pg.getPort(), "postgres", "postgres",
                "postgres", null);
        var latch = new CountDownLatch(1);
        var result = new AtomicReference<String>();
        var debezium = new Debezium();
        debezium.start(cfg,
                record -> {
                    System.out.println(record);

                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        var actualObj = mapper.readTree(record.value());
                        var r = actualObj.get("payload").get("after");
                        result.set(r.toString());
                        System.out.println("*************************************************" + r.toString());

                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    latch.countDown();
                });

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "insert into postevent.test (id, source, type, datacontenttype, dataschema, subject, data, time) "
                            + "values ('1', 'test$source', 'test$type', 'text/plain', 'none', 'test$subject', (decode('013d7d16d7ad4fefb61bd95b765c8ceb', 'hex')), '2024-10-27T22:11:07.937038Z')");
        }
        latch.await(10L, TimeUnit.SECONDS);
        debezium.stop();
        assertEquals(
                "{\"idn\":1,\"id\":\"1\",\"source\":\"test$source\",\"type\":\"test$type\",\"datacontenttype\":\"text/plain\","
                        + "\"dataschema\":\"none\",\"subject\":\"test$subject\",\"data\":\"AT19FtetT++2G9lbdlyM6w==\",\"time\":\"2024-10-27T22:11:07.937038Z\"}",
                result.get());
    }

    void printOffset(Connection conn) throws SQLException {
        System.out.println("--------------------------------OFFSET?");

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM postevent.offsets");
            while (rs.next()) {
                System.out.println("--------------------------------OFFSET--------------------------------");
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
                System.out.println(rs.getString(3));
                System.out.println(rs.getString(4));
                System.out.println("--------------------------------OFFSET--------------------------------");
            }
        }
    }

    @Test
    void debeziumReplicationStopsOnShutdown() throws SQLException, IOException,
            InterruptedException {

        engineReceivesMessage();
        printOffset(conn);
        var active = true;
        while (active) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT active from pg_replication_slots");
                rs.next();
                active = rs.getBoolean(1);
                System.out.println(active);
            }
            if (active)
                Thread.sleep(2000);
        }
        printOffset(conn);
        assertFalse(active, "Replication still active");
    }

}
