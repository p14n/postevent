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

public class DebeziumServer {
        private static final Logger logger = LoggerFactory.getLogger(DebeziumServer.class);

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
                props.setProperty("poll.interval.ms",String.valueOf(pollInterval));
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
