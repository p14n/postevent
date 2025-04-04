package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.processor.OrderedProcessor;
import com.p14n.postevent.telemetry.TelemetryConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

import javax.sql.DataSource;
import java.sql.Connection;

public class TransactionalBroker extends DefaultMessageBroker<Event, TransactionalEvent> {
    private final DataSource ds;

    public TransactionalBroker(DataSource ds, AsyncExecutor asyncExecutor, TelemetryConfig telemetryConfig) {
        super(asyncExecutor, telemetryConfig);
        this.ds = ds;
    }

    public TransactionalBroker(DataSource ds, TelemetryConfig telemetryConfig) {
        super(telemetryConfig);
        this.ds = ds;
    }

    @Override
    public void publish(String topic, Event message) {
        if (!canProcess(topic, message)) {
            return;
        }

        metrics.recordPublished(topic);

        Span span = tracer.spanBuilder("publish_message_tx")
                .setAttribute("topic", topic)
                .setAttribute("event.id", getEventId(message))
                .startSpan();

        // Deliver to all subscribers
        for (MessageSubscriber<TransactionalEvent> subscriber : topicSubscribers.get(topic)) {
            try (Connection c = ds.getConnection()) {
                var op = new OrderedProcessor((connection, event) -> {

                    Span childSpan = tracer.spanBuilder("process_message")
                            .setAttribute("topic", topic)
                            .setAttribute("event.id", getEventId(message))
                            .startSpan();
                    try (Scope childScope = childSpan.makeCurrent()) {
                        subscriber.onMessage(new TransactionalEvent(connection, event));
                        metrics.recordReceived(topic);
                        return true;
                    } catch (Exception e) {
                        childSpan.recordException(e);
                        try {
                            subscriber.onError(e);
                        } catch (Exception ignored) {
                        }
                        return false;
                    } finally {
                        childSpan.end();
                    }

                });
                op.process(c, message);
            } catch (Exception e) {
                try {
                    subscriber.onError(e);
                } catch (Exception ignored) {
                    // If error handling fails, we ignore it to protect other subscribers
                }
            }
        }
    }

    @Override
    protected String getEventId(Event message) {
        return message.id();
    }

    @Override
    public TransactionalEvent convert(Event m) {
        return null;
    }
}
