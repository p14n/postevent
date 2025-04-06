package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;

import io.opentelemetry.api.OpenTelemetry;

public class EventMessageBroker extends DefaultMessageBroker<Event, Event> {

    public EventMessageBroker(AsyncExecutor asyncExecutor, OpenTelemetry ot) {
        super(asyncExecutor, ot);
    }

    public EventMessageBroker(OpenTelemetry ot) {
        super(ot);
    }

    @Override
    public Event convert(Event m) {
        return m;
    }

}
