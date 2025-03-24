package com.p14n.postevent.broker;

public class SystemEventBroker extends
        DefaultMessageBroker<SystemEvent, SystemEvent> implements AutoCloseable {

    @Override
    public SystemEvent convert(SystemEvent m) {
        return m;
    }

}
