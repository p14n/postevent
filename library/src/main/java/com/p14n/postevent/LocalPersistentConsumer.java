package com.p14n.postevent;

import com.p14n.postevent.broker.*;
import com.p14n.postevent.catchup.CatchupServer;
import com.p14n.postevent.catchup.CatchupService;
import com.p14n.postevent.catchup.PersistentBroker;
import com.p14n.postevent.catchup.UnprocessedSubmitter;
import com.p14n.postevent.data.PostEventConfig;
import com.p14n.postevent.data.UnprocessedEventFinder;

import io.opentelemetry.api.OpenTelemetry;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A local consumer implementation that provides persistent event processing
 * with catchup capabilities.
 * This consumer ensures reliable event processing by persisting events and
 * providing mechanisms
 * to handle missed or unprocessed events.
 *
 * <p>
 * Key features:
 * <ul>
 * <li>Persistent event storage using a configured DataSource</li>
 * <li>Automatic catchup for missed events</li>
 * <li>Periodic health checks and unprocessed event processing</li>
 * <li>OpenTelemetry integration for observability</li>
 * <li>Support for batch processing of events</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * var consumer = new LocalPersistentConsumer(dataSource, config, openTelemetry, 100);
 * consumer.start();
 * 
 * // Subscribe to events
 * consumer.subscribe("orders", event -> {
 *     // Process the event
 * });
 * 
 * // When done
 * consumer.close();
 * }</pre>
 */
public class LocalPersistentConsumer implements AutoCloseable, MessageBroker<TransactionalEvent, TransactionalEvent> {
    private static final Logger logger = LoggerFactory.getLogger(LocalPersistentConsumer.class);
    private PostEventConfig cfg;
    private DataSource ds;
    private AsyncExecutor asyncExecutor;
    private TransactionalBroker tb;
    private List<AutoCloseable> closeables;
    private OpenTelemetry ot;
    private final int batchSize;

    /**
     * Creates a new LocalPersistentConsumer with custom executor configuration.
     *
     * @param ds            The DataSource for event persistence
     * @param cfg           The configuration for the consumer
     * @param asyncExecutor The executor for handling asynchronous operations
     * @param ot            OpenTelemetry instance for monitoring
     * @param batchSize     Maximum number of events to process in a batch
     */
    public LocalPersistentConsumer(DataSource ds, PostEventConfig cfg, AsyncExecutor asyncExecutor,
            OpenTelemetry ot, int batchSize) {
        this.ds = ds;
        this.cfg = cfg;
        this.asyncExecutor = asyncExecutor;
        this.ot = ot;
        this.batchSize = batchSize;
    }

    /**
     * Creates a new LocalPersistentConsumer with default executor configuration.
     *
     * @param ds        The DataSource for event persistence
     * @param cfg       The configuration for the consumer
     * @param ot        OpenTelemetry instance for monitoring
     * @param batchSize Maximum number of events to process in a batch
     */
    public LocalPersistentConsumer(DataSource ds, PostEventConfig cfg, OpenTelemetry ot, int batchSize) {
        this(ds, cfg, new DefaultExecutor(2, batchSize), ot, batchSize);
    }

    /**
     * Starts the consumer, initializing all necessary components and scheduling
     * periodic tasks.
     * Sets up the event processing pipeline, catchup service, and unprocessed event
     * handling.
     *
     * @throws IOException          If there's an error starting the consumer
     * @throws InterruptedException If the startup process is interrupted
     * @throws RuntimeException     If the consumer is already started
     */
    public void start() throws IOException, InterruptedException {
        logger.atInfo().log("Starting local persistent consumer");

        if (tb != null) {
            logger.atError().log("Local persistent consumer already started");
            throw new RuntimeException("Already started");
        }

        try {
            var seb = new SystemEventBroker(asyncExecutor, ot);
            tb = new TransactionalBroker(ds, asyncExecutor, ot, seb);
            var pb = new PersistentBroker<>(tb, ds, seb);
            var lc = new LocalConsumer<>(cfg, pb);

            seb.subscribe(new CatchupService(ds, new CatchupServer(ds), seb));
            var unprocessedSubmitter = new UnprocessedSubmitter(seb, ds, new UnprocessedEventFinder(), tb, batchSize);
            seb.subscribe(unprocessedSubmitter);

            asyncExecutor.scheduleAtFixedRate(() -> {
                logger.atDebug().log("Triggering unprocessed check and fetch latest");
                seb.publish(SystemEvent.UnprocessedCheckRequired);
                for (String topic : cfg.topics()) {
                    seb.publish(SystemEvent.FetchLatest.withTopic(topic));
                }
            }, 30, 30, TimeUnit.SECONDS);

            lc.start();
            closeables = List.of(lc, pb, seb, tb, asyncExecutor);

            logger.atInfo().log("Local persistent consumer started successfully");

        } catch (Exception e) {
            logger.atError()
                    .setCause(e)
                    .log("Failed to start local persistent consumer");
            throw e;
        }
    }

    /**
     * Closes all resources associated with this consumer.
     * Ensures proper cleanup of all components in the event processing pipeline.
     */
    @Override
    public void close() {
        logger.atInfo().log("Closing local persistent consumer");

        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception e) {
                logger.atWarn()
                        .setCause(e)
                        .addArgument(c.getClass().getSimpleName())
                        .log("Error closing {}");
            }
        }

        logger.atInfo().log("Local persistent consumer closed");
    }

    /**
     * Publishes a transactional event to the specified topic.
     *
     * @param topic   The topic to publish to
     * @param message The transactional event to publish
     * @throws RuntimeException if publishing fails
     */
    @Override
    public void publish(String topic, TransactionalEvent message) {
        try {
            Publisher.publish(message.event(), message.connection(), message.event().topic());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Subscribes to events on the specified topic.
     *
     * @param topic      The topic to subscribe to
     * @param subscriber The subscriber that will receive events
     * @return true if subscription was successful, false otherwise
     */
    @Override
    public boolean subscribe(String topic, MessageSubscriber<TransactionalEvent> subscriber) {
        return tb.subscribe(topic, subscriber);
    }

    /**
     * Unsubscribes from events on the specified topic.
     *
     * @param topic      The topic to unsubscribe from
     * @param subscriber The subscriber to remove
     * @return true if unsubscription was successful, false otherwise
     */
    @Override
    public boolean unsubscribe(String topic, MessageSubscriber<TransactionalEvent> subscriber) {
        return tb.unsubscribe(topic, subscriber);
    }

    /**
     * Converts a transactional event. In this implementation, returns the event
     * unchanged.
     *
     * @param m The transactional event to convert
     * @return The same transactional event
     */
    @Override
    public TransactionalEvent convert(TransactionalEvent m) {
        return m;
    }
}
