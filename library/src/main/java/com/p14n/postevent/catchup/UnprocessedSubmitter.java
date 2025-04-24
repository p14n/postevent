package com.p14n.postevent.catchup;

import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.broker.SystemEvent;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.data.UnprocessedEventFinder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class UnprocessedSubmitter implements MessageSubscriber<SystemEvent> {

    private final MessageBroker<Event, ?> targetBroker;
    private final DataSource ds;
    private final UnprocessedEventFinder unprocessedEventFinder;
    private final int batchSize;
    private final SystemEventBroker systemEventBroker;

    public UnprocessedSubmitter(SystemEventBroker systemEventBroker, DataSource ds,
            UnprocessedEventFinder unprocessedEventFinder,
            MessageBroker<Event, ?> targetBroker, int batchSize) {
        this.targetBroker = targetBroker;
        this.ds = ds;
        this.unprocessedEventFinder = unprocessedEventFinder;
        this.batchSize = batchSize;
        this.systemEventBroker = systemEventBroker;
    }

    private void resubmit() {
        try (Connection c = ds.getConnection()) {
            var events = unprocessedEventFinder.findUnprocessedEventsWithLimit(c, batchSize);
            for (var e : events) {
                targetBroker.publish(e.topic(), e);
            }
            if (events.size() == batchSize) {
                systemEventBroker.publish(SystemEvent.UnprocessedCheckRequired);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onMessage(SystemEvent message) {
        if (message == SystemEvent.UnprocessedCheckRequired) {
            resubmit();
            ;
        }
    }

}
