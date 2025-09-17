package com.p14n.postevent.example;

import com.p14n.postevent.VertxConsumerServer;
import com.p14n.postevent.VertxPersistentConsumer;
import com.p14n.postevent.adapter.EventBusMessageBroker;
import com.p14n.postevent.broker.DefaultExecutor;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.DatabaseSetup;
import io.opentelemetry.api.OpenTelemetry;
import io.vertx.core.Vertx;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

public class VertxConsumerExample {

    public static void start(DataSource ds) throws IOException, InterruptedException {
        DefaultExecutor executor = new DefaultExecutor(2);
        var ot = OpenTelemetry.noop();
        var vertx = Vertx.vertx();
        var topics = Set.of("order");
        var mb = new EventBusMessageBroker(vertx.eventBus(),ds,executor, ot, "consumer_server");
        var server = new VertxConsumerServer(ds,executor,ot);
        server.start(vertx.eventBus(),mb,topics);

        var client = new VertxPersistentConsumer(ot,executor,20);
        client.start(topics,ds,vertx.eventBus(),mb);
        client.subscribe("order", message -> {
            System.out.println("Got message");
        });

        mb.publish("order", Event.create(UUID.randomUUID().toString(),
                "test",
                "test",
                "text",
                null,
                "test",
                "hello".getBytes(),null));
    }
}
