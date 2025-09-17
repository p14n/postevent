package com.p14n.postevent.vertx.example;

import com.p14n.postevent.vertx.VertxConsumerServer;
import com.p14n.postevent.vertx.VertxPersistentConsumer;
import com.p14n.postevent.vertx.adapter.EventBusMessageBroker;
import com.p14n.postevent.broker.DefaultExecutor;
import com.p14n.postevent.data.Event;
import io.opentelemetry.api.OpenTelemetry;
import io.vertx.core.Vertx;

import javax.sql.DataSource;
import java.io.IOException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

public class VertxConsumerExample {

    public static void start(DataSource ds) throws IOException, InterruptedException {

        DefaultExecutor executor = new DefaultExecutor(2);
        var ot = OpenTelemetry.noop();
        var vertx = Vertx.vertx();
        var topics = Set.of("order");
        var mb = new EventBusMessageBroker(vertx.eventBus(),ds,executor, ot, "consumer_server");
        var server = new VertxConsumerServer(ds,executor,ot);
        server.start(vertx.eventBus(),mb,topics);

        var latch = new CountDownLatch(2);

        var client = new VertxPersistentConsumer(ot,executor,20);
        client.start(topics,ds,vertx.eventBus(),mb);

        mb.publish("order", Event.create(UUID.randomUUID().toString(),
                "test",
                "test",
                "text",
                null,
                "test",
                "hello".getBytes(), Instant.now(),1L ,"order",null));

        client.subscribe("order", message -> {
            System.out.println("Got message");
            latch.countDown();
        });

        mb.publish("order", Event.create(UUID.randomUUID().toString(),
                "test",
                "test",
                "text",
                null,
                "test",
                "hello".getBytes(), Instant.now(),2L ,"order",null));

        latch.await(10, TimeUnit.SECONDS);
    }

    public static void main(String[] args){
        try(var pg = EmbeddedPostgres.start()){
            start(pg.getPostgresDatabase());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
