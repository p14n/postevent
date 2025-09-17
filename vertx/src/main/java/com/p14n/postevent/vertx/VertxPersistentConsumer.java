package com.p14n.postevent.vertx;

import com.p14n.postevent.Publisher;
import com.p14n.postevent.db.DatabaseSetup;
import com.p14n.postevent.vertx.adapter.EventBusMessageBroker;
import com.p14n.postevent.broker.*;
import com.p14n.postevent.catchup.CatchupService;
import com.p14n.postevent.catchup.PersistentBroker;
import com.p14n.postevent.catchup.UnprocessedSubmitter;
import com.p14n.postevent.vertx.client.EventBusCatchupClient;
import com.p14n.postevent.data.UnprocessedEventFinder;
import io.opentelemetry.api.OpenTelemetry;
import io.vertx.core.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class VertxPersistentConsumer implements AutoCloseable, MessageBroker<TransactionalEvent, TransactionalEvent> {

    private static final Logger logger = LoggerFactory.getLogger(VertxPersistentConsumer.class);

    private AsyncExecutor asyncExecutor;
    private List<AutoCloseable> closeables;
    private TransactionalBroker tb;
    SystemEventBroker seb;
    OpenTelemetry ot;
    private final int batchSize;

    public VertxPersistentConsumer(OpenTelemetry ot, AsyncExecutor asyncExecutor, int batchSize) {
        this.asyncExecutor = asyncExecutor;
        this.ot = ot;
        this.batchSize = batchSize;
    }

    public void start(Set<String> topics, DataSource ds,EventBus eb, EventBusMessageBroker mb) {
        logger.atInfo().log("Starting consumer client");

        if (tb != null) {
            logger.atError().log("Consumer client already started");
            throw new IllegalStateException("Already started");
        }
        var db = new DatabaseSetup(ds);
        db.setupClient();

        try {
            seb = new SystemEventBroker(asyncExecutor, ot);
            tb = new TransactionalBroker(ds, asyncExecutor, ot, seb);
            var pb = new PersistentBroker<>(tb, ds, seb);
            //var client = new MessageBrokerGrpcClient(asyncExecutor, ot, channel); // needs fixed threads
            //var catchupClient = new CatchupGrpcClient(channel);

            //Create a client that can listen to system events, register with seb
            //register the pb with the eb for all topics
            var catchupClient = new EventBusCatchupClient(eb);

            for (var topic : topics) {
                mb.subscribeToEventBus(topic,pb);
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

            closeables = List.of(pb, seb, tb);

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
