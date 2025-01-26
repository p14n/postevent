package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class PostgresDebeziumConnectorTest {
    private PostgresDebeziumConnector engine;
    private Properties properties;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        properties = new Properties();
        properties.setProperty("name", "test-connector");
        properties.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        properties.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        properties.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
        properties.setProperty("offset.flush.interval.ms", "1000");
        
        executorService = Executors.newSingleThreadExecutor();
        engine = new PostgresDebeziumConnector(properties, executorService);
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
        executorService.shutdown();
    }

    @Test
    void engineCreatedSuccessfully() {
        assertNotNull(engine);
    }

    @Test
    void throwsExceptionWhenStartedWithoutConsumer() {
        assertThrows(IllegalStateException.class, () -> engine.start());
    }

    @Test
    void engineStartsWithConsumer() {
        AtomicBoolean consumerCalled = new AtomicBoolean(false);
        
        engine.setChangeEventConsumer((ChangeEvent<String, String> event) -> {
            consumerCalled.set(true);
        });

        engine.start();
        
        // Note: In a real test, you would need to trigger a database change
        // and verify that the consumer is called
    }
}
