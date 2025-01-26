package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.sql.SQLException;

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
}
