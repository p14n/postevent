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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalConsumer<OutT> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LocalConsumer.class);
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
        logger.atInfo()
                .addArgument(config.topics()) // renamed from name()
                .log("Starting local consumer for {}");

        try {
            db.setupAll(config.topics()); // renamed from name()
            Consumer<ChangeEvent<String, String>> consumer = record -> {
                try {
                    Event event = changeEventToEvent(record);
                    if (event != null) {
                        broker.publish(event);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to process change event", e);
                }
            };
            debezium.start(config, consumer);
        } catch (IOException | InterruptedException e) {
            logger.atError().setCause(e).log("Failed to start local consumer");
            throw e;
        }
    }

    public void stop() throws IOException {
        debezium.stop();
    }

    @Override
    public void close() throws IOException {
        stop();
    }
}
