package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;

public class EventMessageBroker extends DefaultMessageBroker<Event, Event> {

    public EventMessageBroker(AsyncExecutor asyncExecutor) {
        super(asyncExecutor);
    }

    public EventMessageBroker() {
        super();
    }

    @Override
    public Event convert(Event m) {
        return m;
    }
}
