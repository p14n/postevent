package com.p14n.postevent.example;

import com.p14n.postevent.VertxEventBusConsumer;
import com.p14n.postevent.adapter.EventBusMessageBroker;
import com.p14n.postevent.adapter.EventBusCatchupService;
import com.p14n.postevent.broker.DefaultExecutor;
import com.p14n.postevent.catchup.CatchupServer;
import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.DatabaseSetup;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.opentelemetry.api.OpenTelemetry;
import io.vertx.core.Vertx;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.Set;

/**
 * Example demonstrating how to use the Vert.x EventBus module for event
 * processing.
 * 
 * This example shows:
 * 1. Setting up a server with EventBus message broker and catchup service
 * 2. Creating a consumer that receives events via EventBus
 * 3. Publishing events that are persisted and distributed via EventBus
 * 4. Automatic catchup for guaranteed event delivery
 */
public class VertxEventBusExample {

    public static void main(String[] args) throws Exception {
        // Setup database
        DataSource dataSource = createDataSource();

        // Configuration
        ConfigData config = new ConfigData(
                "vertx-example",
                Set.of("orders", "payments"),
                "localhost",
                5432,
                "postevent",
                "postgres",
                "password");

        // Start server components
        VertxEventBusServer server = new VertxEventBusServer(config, dataSource);
        server.start();

        // Start consumer
        VertxEventBusConsumer consumer = new VertxEventBusConsumer(config, OpenTelemetry.noop(), 30);
        consumer.start(Set.of("orders", "payments"), dataSource);

        // Subscribe to events
        consumer.subscribe("orders", event -> {
            System.out.println("📦 Order Event: " + event.id() + " - " + event.type());
        });

        consumer.subscribe("payments", event -> {
            System.out.println("💳 Payment Event: " + event.id() + " - " + event.type());
        });

        // Publish some test events
        publishTestEvents(server.getMessageBroker());

        // Keep running for demonstration
        System.out.println("🚀 Vert.x EventBus example running...");
        System.out.println("📡 Events are being published and consumed via EventBus");
        System.out.println("🔄 Catchup service ensures no events are missed");
        System.out.println("Press Ctrl+C to stop");

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🛑 Shutting down...");
            try {
                consumer.close();
                server.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        // Keep main thread alive
        Thread.currentThread().join();
    }

    private static void publishTestEvents(EventBusMessageBroker broker) {
        // Publish order events
        for (int i = 1; i <= 5; i++) {
            Event orderEvent = Event.create(
                    "order-" + i,
                    "order-service",
                    "order.created",
                    "application/json",
                    null,
                    "orders",
                    ("Order " + i + " data").getBytes(),
                    null);
            broker.publish("orders", orderEvent);
        }

        // Publish payment events
        for (int i = 1; i <= 3; i++) {
            Event paymentEvent = Event.create(
                    "payment-" + i,
                    "payment-service",
                    "payment.processed",
                    "application/json",
                    null,
                    "payments",
                    ("Payment " + i + " data").getBytes(),
                    null);
            broker.publish("payments", paymentEvent);
        }
    }

    private static DataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/postevent");
        config.setUsername("postgres");
        config.setPassword("password");
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    /**
     * Server component that sets up EventBus message broker and catchup service.
     */
    public static class VertxEventBusServer {
        private final ConfigData config;
        private final DataSource dataSource;
        private final Vertx vertx;

        private EventBusMessageBroker messageBroker;
        private EventBusCatchupService catchupService;

        public VertxEventBusServer(ConfigData config, DataSource dataSource) {
            this.config = config;
            this.dataSource = dataSource;
            this.vertx = Vertx.vertx();
        }

        public void start() throws Exception {
            // Create message broker that publishes to DB + EventBus
            DefaultExecutor executor = new DefaultExecutor(2);
            messageBroker = new EventBusMessageBroker(
                    vertx.eventBus(), dataSource, executor, OpenTelemetry.noop(), "vertx-server");

            // Create catchup service for EventBus requests
            CatchupServer catchupServer = new CatchupServer(dataSource);
            catchupService = new EventBusCatchupService(catchupServer, vertx.eventBus(),Set.of());
            catchupService.start();

            System.out.println("🌐 Vert.x EventBus server started");
        }

        public void stop() throws Exception {
            if (catchupService != null) {
                catchupService.stop();
            }
            if (messageBroker != null) {
                messageBroker.close();
            }
            if (vertx != null) {
                vertx.close();
            }
            System.out.println("🛑 Vert.x EventBus server stopped");
        }

        public EventBusMessageBroker getMessageBroker() {
            return messageBroker;
        }
    }
}
