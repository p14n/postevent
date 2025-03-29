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

public class LocalPersistentConsumer implements AutoCloseable, MessageBroker<TransactionalEvent, TransactionalEvent> {
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
        if (tb != null)
            throw new RuntimeException("Already started");
        tb = new TransactionalBroker(ds, asyncExecutor);
        var seb = new SystemEventBroker(asyncExecutor);
        var pb = new PersistentBroker<>(tb, ds, seb);
        var lc = new LocalConsumer<>(cfg, pb);
        seb.subscribe(new CatchupService(ds, new CatchupServer(ds), seb));
        var unprocessedSubmitter = new UnprocessedSubmitter(ds, new UnprocessedEventFinder(), tb);
        seb.subscribe(unprocessedSubmitter);

        asyncExecutor.scheduleAtFixedRate(() -> {
            seb.publish(SystemEvent.UnprocessedCheckRequired);
        }, 30, 30, TimeUnit.SECONDS);

        lc.start();
        closeables = List.of(lc, pb, seb, tb, asyncExecutor);

    }

    @Override
    public void close() {
        for (var c : closeables) {
            try {
                c.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void publish(TransactionalEvent message) {
        try {
            Publisher.publish(message.event(), message.connection(), cfg.name());
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
