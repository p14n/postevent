package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;

import io.opentelemetry.api.OpenTelemetry;

public class EventMessageBroker extends DefaultMessageBroker<Event, Event> {

    public EventMessageBroker(AsyncExecutor asyncExecutor, OpenTelemetry ot, String scopeName) {
        super(asyncExecutor, ot, scopeName);
    }

    public EventMessageBroker(OpenTelemetry ot, String scopeName) {
        super(ot,scopeName);
    }

    @Override
    public Event convert(Event m) {
        return m;
    }

}
