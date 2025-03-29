package com.p14n.postevent;

import com.p14n.postevent.broker.*;
import com.p14n.postevent.catchup.CatchupServer;
import com.p14n.postevent.catchup.CatchupService;
import com.p14n.postevent.catchup.PersistentBroker;
import com.p14n.postevent.catchup.UnprocessedSubmitter;
import com.p14n.postevent.data.PostEventConfig;
import com.p14n.postevent.data.UnprocessedEventFinder;

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

    public LocalPersistentConsumer(DataSource ds, PostEventConfig cfg, AsyncExecutor asyncExecutor) {
        this.ds = ds;
        this.cfg = cfg;
        this.asyncExecutor = asyncExecutor;
    }

    public LocalPersistentConsumer(DataSource ds, PostEventConfig cfg) {
        this(ds, cfg, new DefaultExecutor(2));
    }

    public void start() throws IOException, InterruptedException {
        logger.atInfo().log("Starting local persistent consumer");

        if (tb != null) {
            logger.atError().log("Local persistent consumer already started");
            throw new RuntimeException("Already started");
        }

        try {
            tb = new TransactionalBroker(ds, asyncExecutor);
            var seb = new SystemEventBroker(asyncExecutor);
            var pb = new PersistentBroker<>(tb, ds, seb);
            var lc = new LocalConsumer<>(cfg, pb);

            seb.subscribe(new CatchupService(ds, new CatchupServer(ds), seb));
            var unprocessedSubmitter = new UnprocessedSubmitter(ds, new UnprocessedEventFinder(), tb);
            seb.subscribe(unprocessedSubmitter);

            asyncExecutor.scheduleAtFixedRate(() -> {
                logger.atDebug().log("Triggering unprocessed check");
                seb.publish(SystemEvent.UnprocessedCheckRequired);
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
    public void publish(TransactionalEvent message) {
        try {
            Publisher.publish(message.event(), message.connection(), message.event().topic()); // renamed from name()
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean subscribe(MessageSubscriber<TransactionalEvent> subscriber) {
        return tb.subscribe(subscriber);
    }

    @Override
    public boolean unsubscribe(MessageSubscriber<TransactionalEvent> subscriber) {
        return tb.unsubscribe(subscriber);
    }

    @Override
    public TransactionalEvent convert(TransactionalEvent m) {
        return m;
    }

}
