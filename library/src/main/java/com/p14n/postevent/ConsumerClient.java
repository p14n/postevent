package com.p14n.postevent;

import com.p14n.postevent.broker.AsyncExecutor;
import com.p14n.postevent.broker.DefaultExecutor;
import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.broker.SystemEvent;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.broker.TransactionalBroker;
import com.p14n.postevent.broker.TransactionalEvent;
import com.p14n.postevent.broker.grpc.MessageBrokerGrpcClient;
import com.p14n.postevent.catchup.CatchupService;
import com.p14n.postevent.catchup.PersistentBroker;
import com.p14n.postevent.catchup.UnprocessedSubmitter;
import com.p14n.postevent.catchup.grpc.CatchupGrpcClient;
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

public class ConsumerClient implements AutoCloseable, MessageBroker<TransactionalEvent, TransactionalEvent> {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);

    private AsyncExecutor asyncExecutor;
    private List<AutoCloseable> closeables;
    private TransactionalBroker tb;
    SystemEventBroker seb;
    OpenTelemetry ot;
    private final int batchSize;

    public ConsumerClient(OpenTelemetry ot, AsyncExecutor asyncExecutor, int batchSize) {
        this.asyncExecutor = asyncExecutor;
        this.ot = ot;
        this.batchSize = batchSize;
    }

    public ConsumerClient(OpenTelemetry ot, int batchSize) {
        this(ot, new DefaultExecutor(2, batchSize), batchSize);
    }

    public void start(Set<String> topics, DataSource ds, String host, int port) {
        start(topics, ds, ManagedChannelBuilder.forAddress(host, port)
                .keepAliveTime(1, TimeUnit.HOURS)
                .keepAliveTimeout(30, TimeUnit.SECONDS)
                .usePlaintext()
                .build());
    }

    public void start(Set<String> topics, DataSource ds, ManagedChannel channel) {
        logger.atInfo().log("Starting consumer client");

        if (tb != null) {
            logger.atError().log("Consumer client already started");
            throw new IllegalStateException("Already started");
        }

        try {
            tb = new TransactionalBroker(ds, asyncExecutor, ot);
            seb = new SystemEventBroker(asyncExecutor, ot);
            var pb = new PersistentBroker<>(tb, ds, seb);
            var client = new MessageBrokerGrpcClient(asyncExecutor, ot, channel); // needs fixed threads
            var catchupClient = new CatchupGrpcClient(channel);

            for (var topic : topics) {
                client.subscribe(topic, pb);
            }
            seb.subscribe(new CatchupService(ds, catchupClient, seb));
            seb.subscribe(new UnprocessedSubmitter(ds, new UnprocessedEventFinder(), tb, batchSize));

            asyncExecutor.scheduleAtFixedRate(
                    () -> seb.publish(SystemEvent.UnprocessedCheckRequired),
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

    @Override
    public void publish(String topic, TransactionalEvent message) {
        try {
            Publisher.publish(message.event(), message.connection(), topic);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean subscribe(String topic, MessageSubscriber<TransactionalEvent> subscriber) {
        var subscribed = tb.subscribe(topic, subscriber);
        seb.publish(SystemEvent.CatchupRequired.withTopic(topic));
        return subscribed;
    }

    @Override
    public boolean unsubscribe(String topic, MessageSubscriber<TransactionalEvent> subscriber) {
        return tb.unsubscribe(topic, subscriber);
    }

    @Override
    public TransactionalEvent convert(TransactionalEvent m) {
        return m;
    }

}
