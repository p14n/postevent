package com.p14n.postevent.broker;

import com.p14n.postevent.telemetry.TelemetryConfig;

public class SystemEventBroker extends
        DefaultMessageBroker<SystemEvent, SystemEvent> {

    public SystemEventBroker(AsyncExecutor asyncExecutor, TelemetryConfig telemetryConfig) {
        super(asyncExecutor,telemetryConfig);
    }

    public SystemEventBroker(TelemetryConfig telemetryConfig) {
        super(telemetryConfig);
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

    @Override
    protected String getEventId(SystemEvent message) {
        return message.toString();
    }
}
