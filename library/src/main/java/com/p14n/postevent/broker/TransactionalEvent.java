package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.data.Traceable;

import java.sql.Connection;

public record TransactionalEvent(Connection connection, Event event) implements Traceable {

    @Override
    public String id() {
        return event.id();
    }

    @Override
    public String topic() {
        return event.topic();
    }

    @Override
    public String subject() {
        return event.subject();
    }

    @Override
    public String traceparent() {
        return event.traceparent();
    }
}
