package com.p14n.postevent.broker;

import io.opentelemetry.api.OpenTelemetry;

public class SystemEventBroker extends
        DefaultMessageBroker<SystemEvent, SystemEvent> {

    public SystemEventBroker(AsyncExecutor asyncExecutor, OpenTelemetry ot) {
        super(asyncExecutor, ot);
    }

    public SystemEventBroker(OpenTelemetry ot) {
        super(ot);
    }

    @Override
    public SystemEvent convert(SystemEvent m) {
        return m;
    }

    public void publish(SystemEvent event) {
        publish("system", event);
    }

    public void subscribe(MessageSubscriber<SystemEvent> subscriber) {
        subscribe("system", subscriber);
    }

}
