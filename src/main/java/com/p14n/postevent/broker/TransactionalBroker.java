package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.processor.OrderedProcessor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.function.BiFunction;

public class TransactionalBroker extends DefaultMessageBroker<Event,TransactionalEvent> {
    private final DataSource ds;

    public TransactionalBroker(DataSource ds){
        this.ds = ds;
    }
    @Override
    public void publish(Event message) {

        if (!canProcess(message)) {
            return;
        }

        // Deliver to all subscribers
        for (MessageSubscriber<TransactionalEvent> subscriber : subscribers) {
            try (Connection c = ds.getConnection()){
                var op = new OrderedProcessor((connection, event) -> {
                    try {
                        subscriber.onMessage(new TransactionalEvent(connection,event));
                        return true;
                    } catch (Exception e){
                        return false;
                    }
                });
                op.process(c,message);
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
