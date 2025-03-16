package com.p14n.postevent.broker;

public class SystemEventBroker extends DefaultMessageBroker<SystemEventBroker.SystemEvent,SystemEventBroker.SystemEvent> implements AutoCloseable{

    @Override
    public SystemEvent convert(SystemEvent m) {
        return m;
    }

    public enum SystemEvent {
        CatchupRequired
    }
}
