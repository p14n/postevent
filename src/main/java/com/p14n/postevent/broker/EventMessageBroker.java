package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;

public class EventMessageBroker extends DefaultMessageBroker<Event,Event> {
    @Override
    public Event convert(Event m) {
        return m;
    }
}
