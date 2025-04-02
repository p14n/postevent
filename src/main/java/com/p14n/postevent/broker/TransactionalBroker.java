package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.processor.OrderedProcessor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

public class TransactionalBroker extends DefaultMessageBroker<Event, TransactionalEvent> {
    private final DataSource ds;

    public TransactionalBroker(DataSource ds, AsyncExecutor asyncExecutor) {
        super(asyncExecutor);
        this.ds = ds;
    }

    public TransactionalBroker(DataSource ds) {
        super();
        this.ds = ds;
    }

    @Override
    public void publish(String topic, Event message) {
        if (!canProcess(topic, message)) {
            return;
        }

        // Deliver to all subscribers
        for (MessageSubscriber<TransactionalEvent> subscriber : topicSubscribers.get(topic)) {
            try (Connection c = ds.getConnection()) {
                var op = new OrderedProcessor((connection, event) -> {
                    try {
                        subscriber.onMessage(new TransactionalEvent(connection, event));
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                });
                op.process(c, message);
            } catch (Exception e) {
                try {
                    subscriber.onError(e);
                } catch (Exception ignored) {
                    // If error handling fails, we ignore it to protect other subscribers
                }
            }
        }
    }

    @Override
    public TransactionalEvent convert(Event m) {
        return null;
    }
}
