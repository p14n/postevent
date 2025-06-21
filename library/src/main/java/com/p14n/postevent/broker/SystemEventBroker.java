package com.p14n.postevent.broker;

import io.opentelemetry.api.OpenTelemetry;

public class SystemEventBroker extends
        DefaultMessageBroker<SystemEvent, SystemEvent> {

    public SystemEventBroker(AsyncExecutor asyncExecutor, OpenTelemetry ot) {
        super(asyncExecutor, ot, "system_events");
    }

    public SystemEventBroker(OpenTelemetry ot) {
        super(ot,"system_events");
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

    public void triggerFetchLatest(String topic) {
        publish(SystemEvent.FetchLatest.withTopic(topic));
    }

}
