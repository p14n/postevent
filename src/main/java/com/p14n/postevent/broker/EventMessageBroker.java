package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.telemetry.TelemetryConfig;

public class EventMessageBroker extends DefaultMessageBroker<Event, Event> {

    public EventMessageBroker(AsyncExecutor asyncExecutor, TelemetryConfig telemetryConfig) {
        super(asyncExecutor,telemetryConfig);
    }

    public EventMessageBroker(TelemetryConfig telemetryConfig) {
        super(telemetryConfig);
    }

    @Override
    public Event convert(Event m) {
        return m;
    }

    @Override
    protected String getEventId(Event message) {
        return message.id();
    }
}
