package com.p14n.postevent;

import com.p14n.postevent.broker.AsyncExecutor;
import com.p14n.postevent.broker.DefaultExecutor;
import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.broker.SystemEvent;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.broker.TransactionalBroker;
import com.p14n.postevent.broker.TransactionalEvent;
import com.p14n.postevent.broker.remote.MessageBrokerGrpcClient;
import com.p14n.postevent.catchup.CatchupService;
import com.p14n.postevent.catchup.PersistentBroker;
import com.p14n.postevent.catchup.UnprocessedSubmitter;
import com.p14n.postevent.catchup.remote.CatchupGrpcClient;
import com.p14n.postevent.data.UnprocessedEventFinder;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A remote consumer implementation that provides persistent event processing
 * with catchup capabilities.
 * This consumer connects to a remote event source via gRPC and ensures reliable
 * event processing
 * with persistence and automatic catchup for missed events.
 *
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Remote event consumption via gRPC</li>
 * <li>Persistent event storage</li>
 * <li>Automatic catchup for missed events</li>
 * <li>Periodic health checks and catchup triggers</li>
 * <li>OpenTelemetry integration for observability</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * var consumer = new RemotePersistentConsumer(openTelemetry, 100);
 * consumer.start(Set.of("orders", "inventory"), dataSource, "localhost", 8080);
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
public class RemotePersistentConsumer implements AutoCloseable, MessageBroker<TransactionalEvent, TransactionalEvent> {
    private static final Logger logger = LoggerFactory.getLogger(RemotePersistentConsumer.class);

    private AsyncExecutor asyncExecutor;
    private List<AutoCloseable> closeables;
    private TransactionalBroker tb;
    SystemEventBroker seb;
    OpenTelemetry ot;
    private final int batchSize;

    /**
     * Creates a new RemotePersistentConsumer with custom executor configuration.
     *
     * @param ot            OpenTelemetry instance for monitoring
     * @param asyncExecutor Executor for handling asynchronous operations
     * @param batchSize     Maximum number of events to process in a batch
     */
    public RemotePersistentConsumer(OpenTelemetry ot, AsyncExecutor asyncExecutor, int batchSize) {
        this.asyncExecutor = asyncExecutor;
        this.ot = ot;
        this.batchSize = batchSize;
    }

    /**
     * Creates a new RemotePersistentConsumer with default executor configuration.
     *
     * @param ot        OpenTelemetry instance for monitoring
     * @param batchSize Maximum number of events to process in a batch
     */
    public RemotePersistentConsumer(OpenTelemetry ot, int batchSize) {
        this(ot, new DefaultExecutor(2, batchSize), batchSize);
    }

    /**
     * Starts the consumer with the specified configuration.
     *
     * @param topics Set of topics to subscribe to
     * @param ds     DataSource for event persistence
     * @param host   Remote host address
     * @param port   Remote host port
     */
    public void start(Set<String> topics, DataSource ds, String host, int port) {
        start(topics, ds, ManagedChannelBuilder.forAddress(host, port)
                .keepAliveTime(1, TimeUnit.HOURS)
                .keepAliveTimeout(30, TimeUnit.SECONDS)
                .usePlaintext()
                .build());
    }

    /**
     * Starts the consumer with a pre-configured gRPC channel.
     *
     * @param topics  Set of topics to subscribe to
     * @param ds      DataSource for event persistence
     * @param channel Configured gRPC channel
     * @throws IllegalStateException if the consumer is already started
     */
    public void start(Set<String> topics, DataSource ds, ManagedChannel channel) {
        logger.atInfo().log("Starting consumer client");

        if (tb != null) {
            logger.atError().log("Consumer client already started");
            throw new IllegalStateException("Already started");
        }

        try {
            seb = new SystemEventBroker(asyncExecutor, ot);
            tb = new TransactionalBroker(ds, asyncExecutor, ot, seb);
            var pb = new PersistentBroker<>(tb, ds, seb);
            var client = new MessageBrokerGrpcClient(asyncExecutor, ot, channel); // needs fixed threads
            var catchupClient = new CatchupGrpcClient(channel);

            for (var topic : topics) {
                client.subscribe(topic, pb);
            }
            seb.subscribe(new CatchupService(ds, catchupClient, seb));
            seb.subscribe(new UnprocessedSubmitter(seb, ds, new UnprocessedEventFinder(), tb, batchSize));

            asyncExecutor.scheduleAtFixedRate(
                    () -> {
                        seb.publish(SystemEvent.UnprocessedCheckRequired);
                        for (String topic : topics) {
                            seb.publish(SystemEvent.FetchLatest.withTopic(topic));
                        }
                    },
                    30, 30, TimeUnit.SECONDS);

            closeables = List.of(client, catchupClient, pb, seb, tb);

            logger.atInfo().log("Consumer client started successfully");

        } catch (Exception e) {
            logger.atError()
                    .setCause(e)
                    .log("Failed to start consumer client");
            throw new RuntimeException("Failed to start consumer client", e);
        }
    }

    /**
     * Closes all resources associated with this consumer.
     * This includes the message brokers, gRPC channel, and other closeable
     * resources.
     */
    @Override
    public void close() {
        logger.atInfo().log("Closing consumer client");

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

        logger.atInfo().log("Consumer client closed");
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
            Publisher.publish(message.event(), message.connection(), topic);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Subscribes to events on the specified topic.
     * Triggers a catchup event to ensure the subscriber receives any missed events.
     *
     * @param topic      The topic to subscribe to
     * @param subscriber The subscriber that will receive events
     * @return true if subscription was successful, false otherwise
     */
    @Override
    public boolean subscribe(String topic, MessageSubscriber<TransactionalEvent> subscriber) {
        var subscribed = tb.subscribe(topic, subscriber);
        seb.publish(SystemEvent.CatchupRequired.withTopic(topic));
        return subscribed;
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
