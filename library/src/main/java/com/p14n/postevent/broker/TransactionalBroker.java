package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.processor.OrderedProcessor;

import io.opentelemetry.api.OpenTelemetry;

import javax.sql.DataSource;

import static com.p14n.postevent.telemetry.OpenTelemetryFunctions.processWithTelemetry;

import java.sql.Connection;

public class TransactionalBroker extends DefaultMessageBroker<Event, TransactionalEvent> {
    private final DataSource ds;

    public TransactionalBroker(DataSource ds, AsyncExecutor asyncExecutor, OpenTelemetry ot) {
        super(asyncExecutor, ot,"transactional_broker");
        this.ds = ds;
    }

    public TransactionalBroker(DataSource ds, OpenTelemetry ot) {
        super(ot, "transactional_broker");
        this.ds = ds;
    }

    @Override
    public void publish(String topic, Event message) {
        if (!canProcess(topic, message)) {
            return;
        }

        metrics.recordPublished(topic);

        // Deliver to all subscribers
        for (MessageSubscriber<TransactionalEvent> subscriber : topicSubscribers.get(topic)) {
            try (Connection c = ds.getConnection()) {

                processWithTelemetry(openTelemetry,tracer, message, "ordered_process", () -> {

                    var op = new OrderedProcessor((connection, event) -> {

                        return processWithTelemetry(openTelemetry,tracer, message, "message_transaction", () -> {
                            try {
                                subscriber.onMessage(new TransactionalEvent(connection, event));
                                metrics.recordReceived(topic);
                                return true;
                            } catch (Exception e) {
                                try {
                                    subscriber.onError(e);
                                } catch (Exception ignored) {
                                }
                                return false;
                            }
                        });

                    });
                    op.process(c, message);
                    return null;
                });

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
    public TransactionalEvent convert(Event m) {
        return null;
    }
}
