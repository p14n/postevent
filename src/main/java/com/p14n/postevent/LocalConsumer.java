package com.p14n.postevent;

import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.data.PostEventConfig;
import com.p14n.postevent.debezium.DebeziumServer;
import static com.p14n.postevent.debezium.Functions.changeEventToEvent;
import io.debezium.engine.ChangeEvent;
import java.io.IOException;
import java.util.function.Consumer;

public class LocalConsumer {
    private final DebeziumServer debezium;
    private final MessageBroker<Event> broker;
    private final PostEventConfig config;

    public LocalConsumer(PostEventConfig config, MessageBroker<Event> broker) {
        this.config = config;
        this.broker = broker;
        this.debezium = new DebeziumServer();
    }

    public void start() throws IOException, InterruptedException {
        Consumer<ChangeEvent<String, String>> consumer = record -> {
            try {
                broker.publish(changeEventToEvent(record));
            } catch (Exception e) {
                throw new RuntimeException("Failed to process change event", e);
            }
        };
        debezium.start(config, consumer);
    }

    public void stop() throws IOException {
        debezium.stop();
    }
}