package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class PostgresDebeziumConnector {
    private final Properties properties;
    private final ExecutorService executorService;
    private Consumer<ChangeEvent<String, String>> changeEventConsumer;
    private DebeziumEngine<ChangeEvent<String, String>> engine;

    public PostgresDebeziumConnector(Properties properties, ExecutorService executorService) {
        this.properties = properties;
        this.executorService = executorService;
    }

    public void setChangeEventConsumer(Consumer<ChangeEvent<String, String>> consumer) {
        this.changeEventConsumer = consumer;
    }

    public void start() {
        if (changeEventConsumer == null) {
            throw new IllegalStateException("Change event consumer must be set before starting the engine");
        }

        this.engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .notifying(changeEventConsumer)
                .build();

        executorService.submit(engine);
    }

    public void close() {
        if (engine != null) {
            try {
                engine.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close Debezium engine", e);
            }
        }
    }
}
