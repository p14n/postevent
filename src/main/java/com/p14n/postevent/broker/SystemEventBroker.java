package com.p14n.postevent.broker;

public class SystemEventBroker extends
        DefaultMessageBroker<SystemEvent, SystemEvent> {

    public SystemEventBroker(AsyncExecutor asyncExecutor) {
        super(asyncExecutor);
    }

    public SystemEventBroker() {
        super();
    }

    @Override
    public SystemEvent convert(SystemEvent m) {
        return m;
    }

}
