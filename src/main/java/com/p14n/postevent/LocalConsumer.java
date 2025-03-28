package com.p14n.postevent;

import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.data.PostEventConfig;
import com.p14n.postevent.db.DatabaseSetup;
import com.p14n.postevent.debezium.DebeziumServer;
import static com.p14n.postevent.debezium.Functions.changeEventToEvent;
import io.debezium.engine.ChangeEvent;
import java.io.IOException;
import java.util.function.Consumer;

public class LocalConsumer<OutT> implements AutoCloseable {
    private final DebeziumServer debezium;
    private final MessageBroker<Event, OutT> broker;
    private final PostEventConfig config;
    private final DatabaseSetup db;

    public LocalConsumer(PostEventConfig config, MessageBroker<Event, OutT> broker) {
        this.config = config;
        this.broker = broker;
        this.db = new DatabaseSetup(config);
        this.debezium = new DebeziumServer();
    }

    public void start() throws IOException, InterruptedException {
        db.setupAll(config.name());
        Consumer<ChangeEvent<String, String>> consumer = record -> {
            try {
                Event event = changeEventToEvent(record);
                System.err.println("LC got event " + event.id());
                if (event != null) {
                    broker.publish(event);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to process change event", e);
            }
        };
        debezium.start(config, consumer);
    }

    public void stop() throws IOException {
        debezium.stop();
    }

    @Override
    public void close() throws IOException {
        stop();
    }
}