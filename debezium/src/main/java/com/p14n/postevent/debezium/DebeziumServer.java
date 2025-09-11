package com.p14n.postevent.debezium;

import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.p14n.postevent.data.PostEventConfig;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.ChangeEvent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server implementation for Debezium Change Data Capture (CDC).
 * Manages a Debezium engine instance to capture database changes and forward
 * them to a consumer.
 * Supports PostgreSQL CDC using the pgoutput plugin with configurable topics
 * and affinity.
 *
 * <p>
 * The server uses JDBC offset storage for tracking progress and supports
 * filtered publication mode.
 * It automatically manages replication slots and cleanup on shutdown.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * PostEventConfig config = // initialize configuration
 * DebeziumServer server = new DebeziumServer();
 *
 * // Start the server with a consumer
 * server.start(config, event -> {
 *     // Process the change event
 *     System.out.println("Received change: " + event);
 * });
 *
 * // Shutdown when done
 * server.stop();
 * }</pre>
 */
public class DebeziumServer {
        private static final Logger logger = LoggerFactory.getLogger(DebeziumServer.class);

        /**
         * Constructs a new DebeziumServer instance.
         */
        public DebeziumServer() {
        }

        /**
         * Creates Debezium configuration properties for PostgreSQL CDC.
         *
         * <p>
         * Configures:
         * </p>
         * <ul>
         * <li>PostgreSQL connector with pgoutput plugin</li>
         * <li>JDBC offset storage with affinity-based partitioning</li>
         * <li>Filtered publication mode for specified topics</li>
         * <li>Automatic replication slot management</li>
         * </ul>
         *
         * @param affinity     Identifier for this instance, used for offset tracking
         * @param topics       Set of topics to monitor for changes
         * @param dbHost       Database host address
         * @param dbPort       Database port
         * @param dbUser       Database username
         * @param dbPassword   Database password
         * @param dbName       Database name
         * @param pollInterval Interval in milliseconds between polls
         * @return Properties configured for Debezium PostgreSQL connector
         */
        public static Properties props(
                        String affinity,
                        Set<String> topics,
                        String dbHost,
                        String dbPort,
                        String dbUser,
                        String dbPassword,
                        String dbName,
                        int pollInterval) {
                final Properties props = new Properties();

                // Create comma-separated list of tables
                String tableList = topics.stream()
                                .map(topic -> "postevent." + topic)
                                .collect(Collectors.joining(","));

                props.setProperty("name", "postevent-multi");
                props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
                props.setProperty("offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore");
                props.setProperty("offset.storage.jdbc.offset.table.name", "postevent.offsets");
                props.setProperty("offset.storage.jdbc.url",
                                "jdbc:postgresql://%s:%s/%s".formatted(dbHost, dbPort, dbName));
                props.setProperty("offset.storage.jdbc.user", dbUser);
                props.setProperty("offset.storage.jdbc.password", dbPassword);
                props.setProperty("offset.flush.interval.ms", "1000");
                props.setProperty("poll.interval.ms", String.valueOf(pollInterval));
                props.setProperty("database.hostname", dbHost);
                props.setProperty("plugin.name", "pgoutput");
                props.setProperty("database.port", dbPort);
                props.setProperty("database.user", dbUser);
                props.setProperty("database.password", dbPassword);
                props.setProperty("database.dbname", dbName);
                props.setProperty("table.include.list", tableList);
                props.setProperty("topic.prefix", "postevent");
                props.setProperty("publication.autocreate.mode", "filtered");
                props.setProperty("snapshot.mode", "no_data");
                props.setProperty("slot.name", "postevent_" + affinity);
                props.setProperty("slot.drop.on.stop", "true");
                props.setProperty("offset.storage.jdbc.offset.table.ddl",
                                "CREATE TABLE IF NOT EXISTS %s (affinityid VARCHAR(255) NOT NULL, id VARCHAR(36) NOT NULL, "
                                                +
                                                "offset_key VARCHAR(1255), offset_val VARCHAR(1255)," +
                                                "record_insert_ts TIMESTAMP NOT NULL," +
                                                "record_insert_seq INTEGER NOT NULL" +
                                                ")");
                props.setProperty("offset.storage.jdbc.offset.table.select",
                                "SELECT id, offset_key, offset_val FROM %s WHERE affinityid = '" + affinity
                                                + "' ORDER BY record_insert_ts, record_insert_seq");
                props.setProperty("offset.storage.jdbc.offset.table.delete",
                                "DELETE FROM %s WHERE affinityid = '" + affinity + "'");
                props.setProperty("offset.storage.jdbc.offset.table.insert",
                                "INSERT INTO %s(affinityid, id, offset_key, offset_val, record_insert_ts, record_insert_seq) VALUES ( '"
                                                + affinity + "', ?, ?, ?, ?, ? )");
                return props;
        }

        private ExecutorService executor;
        private DebeziumEngine<ChangeEvent<String, String>> engine;

        /**
         * Starts the Debezium engine with the specified configuration and consumer.
         *
         * <p>
         * Initializes a single-threaded executor and configures the Debezium engine to:
         * <ul>
         * <li>Use JSON format for change events</li>
         * <li>Monitor configured topics for changes</li>
         * <li>Forward events to the provided consumer</li>
         * </ul>
         *
         * @param cfg      Configuration for the Debezium engine
         * @param consumer Consumer to process change events
         * @throws IllegalStateException if the consumer is null, config is null, or
         *                               startup timeout is exceeded
         * @throws IOException           if engine initialization fails
         * @throws InterruptedException  if startup is interrupted
         */
        public void start(PostEventConfig cfg,
                        Consumer<ChangeEvent<String, String>> consumer) throws IOException, InterruptedException {
                if (consumer == null) {
                        throw new IllegalStateException("Change event consumer must be set before starting the engine");
                }
                if (cfg == null) {
                        throw new IllegalStateException("Config must be set before starting the engine");
                }
                logger.atInfo()
                                .addArgument(cfg.topics())
                                .addArgument(cfg.affinity())
                                .log("Starting Debezium engine for topics {} with affinity {}");
                var started = new CountDownLatch(1);
                engine = DebeziumEngine.create(Json.class)
                                .using(new DebeziumEngine.ConnectorCallback() {
                                        @Override
                                        public void taskStarted() {
                                                DebeziumEngine.ConnectorCallback.super.connectorStarted();
                                                started.countDown();
                                        }
                                })
                                .using(cfg.overrideProps() != null ? cfg.overrideProps()
                                                : props(cfg.affinity(), cfg.topics(), cfg.dbHost(),
                                                                String.valueOf(cfg.dbPort()), cfg.dbUser(),
                                                                cfg.dbPassword(),
                                                                cfg.dbName(), cfg.pollInterval()))
                                .notifying(consumer)
                                .build();
                executor = Executors.newSingleThreadExecutor(
                                new ThreadFactoryBuilder().setNameFormat("post-event-debezium-%d").build());
                executor.execute(engine);
                if (!started.await(cfg.startupTimeoutSeconds(), TimeUnit.SECONDS)) {
                        logger.atError().log("Debezium engine failed to start within {} seconds",
                                        cfg.startupTimeoutSeconds());
                        throw new IllegalStateException("Debezium engine failed to start within "
                                        + cfg.startupTimeoutSeconds() + " seconds");
                }
                logger.atInfo().log("Debezium engine started successfully");
        }

        /**
         * Stops the Debezium engine and executor service.
         * Ensures clean shutdown of resources with a 5-second timeout.
         *
         * @throws IOException if engine shutdown fails
         */
        public void stop() throws IOException {
                if (executor != null) {
                        executor.shutdown();
                }
                if (engine != null) {
                        engine.close();
                }
                if (executor != null) {
                        executor.shutdownNow();
                        try {
                                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                                        logger.warn("Executor did not terminate in the specified time.");
                                }
                        } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                logger.error("Shutdown interrupted", e);
                        }
                }
        }
}
