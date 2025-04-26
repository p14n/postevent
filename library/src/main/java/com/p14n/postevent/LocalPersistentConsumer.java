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

public class LocalPersistentConsumer implements AutoCloseable, MessageBroker<TransactionalEvent, TransactionalEvent> {
    private static final Logger logger = LoggerFactory.getLogger(LocalPersistentConsumer.class);
    private PostEventConfig cfg;
    private DataSource ds;
    private AsyncExecutor asyncExecutor;
    private TransactionalBroker tb;
    private List<AutoCloseable> closeables;
    private OpenTelemetry ot;
    private final int batchSize;

    public LocalPersistentConsumer(DataSource ds, PostEventConfig cfg, AsyncExecutor asyncExecutor,
            OpenTelemetry ot, int batchSize) {
        this.ds = ds;
        this.cfg = cfg;
        this.asyncExecutor = asyncExecutor;
        this.ot = ot;
        this.batchSize = batchSize;
    }

    public LocalPersistentConsumer(DataSource ds, PostEventConfig cfg, OpenTelemetry ot, int batchSize) {
        this(ds, cfg, new DefaultExecutor(2, batchSize), ot, batchSize);
    }

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

    @Override
    public void publish(String topic, TransactionalEvent message) {
        try {
            Publisher.publish(message.event(), message.connection(), message.event().topic()); // renamed from name()
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean subscribe(String topic, MessageSubscriber<TransactionalEvent> subscriber) {
        return tb.subscribe(topic, subscriber);
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
