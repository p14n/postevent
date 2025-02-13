package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import java.io.IOException;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class LocalConsumer {
    private final Debezium debezium;
    private final MessageBroker<Event> broker;
    private final PostEventConfig config;
    private final ObjectMapper mapper = new ObjectMapper();

    public LocalConsumer(PostEventConfig config, MessageBroker<Event> broker) {
        this.config = config;
        this.broker = broker;
        this.debezium = new Debezium();
    }

    public void start() throws IOException, InterruptedException {
        Consumer<ChangeEvent<String, String>> consumer = record -> {
            try {
                var actualObj = mapper.readTree(record.value());
                var r = actualObj.get("payload").get("after");

                broker.publish(Event.create(r.get("id").asText(),
                        r.get("source").asText(),
                        r.get("type").asText(),
                        r.get("datacontenttype").asText(),
                        r.get("dataschema").asText(),
                        r.get("subject").asText(),
                        r.get("data").binaryValue()));

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