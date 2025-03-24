package com.p14n.postevent;

import com.p14n.postevent.broker.AsyncExecutor;
import com.p14n.postevent.broker.DefaultExecutor;
import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
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

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ConsumerClient implements AutoCloseable, MessageBroker<TransactionalEvent, TransactionalEvent> {

    private AsyncExecutor asyncExecutor;
    private List<AutoCloseable> closeables;
    private TransactionalBroker tb;
    private String topic;

    public ConsumerClient(String topic, AsyncExecutor asyncExecutor) {
        this.asyncExecutor = asyncExecutor;
        this.topic = topic;
    }

    public ConsumerClient(String topic) {
        this(topic, new DefaultExecutor(2));
    }

    public void start(DataSource ds, String host, int port) {
        start(ds, ManagedChannelBuilder.forAddress(host, port)
                .keepAliveTime(1, TimeUnit.HOURS)
                .keepAliveTimeout(30, TimeUnit.SECONDS)
                .usePlaintext()
                .build());
    }

    public void start(DataSource ds, ManagedChannel channel) {
        if (tb != null) {
            throw new IllegalStateException("Already started");
        }
        tb = new TransactionalBroker(ds);
        var seb = new SystemEventBroker(asyncExecutor);
        var pb = new PersistentBroker<>(tb, ds, seb);
        var client = new MessageBrokerGrpcClient(channel, topic);
        var catchupClient = new CatchupGrpcClient(channel);
        client.subscribe(pb);

        seb.subscribe(new CatchupService(ds, catchupClient, seb));
        seb.subscribe(new UnprocessedSubmitter(ds, new UnprocessedEventFinder(), tb));

        closeables = List.of(client, catchupClient, pb, seb, tb);

    }

    @Override
    public void close() {
        for (AutoCloseable c : closeables) {
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
            Publisher.publish(message.event(), message.connection(), topic);
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
