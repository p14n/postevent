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

/**
 * A local consumer implementation that captures database changes using Debezium
 * and publishes them to a message broker.
 *
 * <p>
 * This consumer:
 * <ul>
 * <li>Sets up and manages database connections for change data capture</li>
 * <li>Initializes and controls a Debezium server instance</li>
 * <li>Converts Debezium change events to application events</li>
 * <li>Publishes converted events to a configured message broker</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * PostEventConfig config = new PostEventConfig(...);
 * MessageBroker<Event, OutT> broker = new EventMessageBroker(...);
 * 
 * LocalConsumer<OutT> consumer = new LocalConsumer<>(config, broker);
 * consumer.start();
 * 
 * // When done
 * consumer.close();
 * }</pre>
 *
 * @param <OutT> The type of messages that the broker will publish
 */
public class LocalConsumer<OutT> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LocalConsumer.class);
    private final DebeziumServer debezium;
    private final MessageBroker<Event, OutT> broker;
    private final PostEventConfig config;
    private final DatabaseSetup db;

    /**
     * Creates a new LocalConsumer instance.
     *
     * @param config The configuration for the consumer and database connection
     * @param broker The message broker to publish events to
     */
    public LocalConsumer(PostEventConfig config, MessageBroker<Event, OutT> broker) {
        this.config = config;
        this.broker = broker;
        this.db = new DatabaseSetup(config);
        this.debezium = new DebeziumServer();
    }

    /**
     * Starts the consumer by setting up the database and initializing the Debezium
     * server.
     * 
     * <p>
     * This method:
     * <ul>
     * <li>Sets up database tables and configurations for the specified topics</li>
     * <li>Configures and starts the Debezium server</li>
     * <li>Sets up event processing pipeline from Debezium to the message
     * broker</li>
     * </ul>
     *
     * @throws IOException          If there's an error starting the consumer or
     *                              connecting to the database
     * @throws InterruptedException If the startup process is interrupted
     */
    public void start() throws IOException, InterruptedException {
        logger.atInfo()
                .addArgument(config.topics())
                .log("Starting local consumer for {}");

        try {
            db.setupAll(config.topics());
            Consumer<ChangeEvent<String, String>> consumer = record -> {
                try {
                    Event event = changeEventToEvent(record);
                    if (event != null) {
                        broker.publish(event.topic(), event);
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

    /**
     * Stops the Debezium server and releases associated resources.
     *
     * @throws IOException If there's an error stopping the server
     */
    public void stop() throws IOException {
        debezium.stop();
    }

    /**
     * Implements AutoCloseable to ensure proper resource cleanup.
     * Delegates to {@link #stop()}.
     *
     * @throws IOException If there's an error during cleanup
     */
    @Override
    public void close() throws IOException {
        stop();
    }
}
