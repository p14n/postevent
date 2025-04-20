package com.p14n.postevent.broker;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import com.p14n.postevent.data.Traceable;
import com.p14n.postevent.telemetry.BrokerMetrics;
import static com.p14n.postevent.telemetry.OpenTelemetryFunctions.processWithTelemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;

public abstract class DefaultMessageBroker<InT extends Traceable, OutT>
        implements MessageBroker<InT, OutT>, AutoCloseable {

    protected final ConcurrentHashMap<String, Set<MessageSubscriber<OutT>>> topicSubscribers = new ConcurrentHashMap<>();
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    private final AsyncExecutor asyncExecutor;
    protected final BrokerMetrics metrics;
    protected final Tracer tracer;

    protected final OpenTelemetry openTelemetry;

    public DefaultMessageBroker(OpenTelemetry ot,String scopeName) {
        this(new DefaultExecutor(2), ot,scopeName);
    }

    public DefaultMessageBroker(AsyncExecutor asyncExecutor, OpenTelemetry ot,String scopeName) {
        this.asyncExecutor = asyncExecutor;
        this.metrics = new BrokerMetrics(ot.getMeter(scopeName));
        this.tracer = ot.getTracer(scopeName);
        this.openTelemetry = ot;
    }

    protected boolean canProcess(String topic, InT message) {
        if (closed.get()) {
            throw new IllegalStateException("Broker is closed");
        }

        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        if (topic == null) {
            throw new IllegalArgumentException("Topic cannot be null");
        }

        // If no subscribers for this topic, message is silently dropped
        return topicSubscribers.containsKey(topic) && !topicSubscribers.get(topic).isEmpty();
    }

    @Override
    public void publish(String topic, InT message) {
        if (!canProcess(topic, message)) {
            return;
        }

        metrics.recordPublished(topic);

        // Deliver to all subscribers for this topic
        Set<MessageSubscriber<OutT>> subscribers = topicSubscribers.get(topic);
        if (subscribers != null) {

            processWithTelemetry(openTelemetry,tracer, message, "publish_message", () -> {
                for (MessageSubscriber<OutT> subscriber : subscribers) {
                    asyncExecutor.submit(() -> processWithTelemetry(openTelemetry,tracer, message, "process_message",
                            () -> {
                                try {
                                    subscriber.onMessage(convert(message));
                                    metrics.recordReceived(topic);
                                    return true;
                                } catch (Exception e) {
                                    try {
                                        subscriber.onError(e);
                                    } catch (Exception ignored) {
                                    }
                                    throw e;
                                }
                            }));
                }
                return null;
            });

        }
    }

    @Override
    public boolean subscribe(String topic, MessageSubscriber<OutT> subscriber) {
        if (closed.get()) {
            throw new IllegalStateException("Broker is closed");
        }

        if (subscriber == null) {
            throw new IllegalArgumentException("Subscriber cannot be null");
        }

        if (topic == null) {
            throw new IllegalArgumentException("Topic cannot be null");
        }

        boolean added = topicSubscribers
                .computeIfAbsent(topic, k -> new CopyOnWriteArraySet<>())
                .add(subscriber);

        if (added) {
            metrics.recordSubscriberAdded(topic);
        }

        return added;
    }

    @Override
    public boolean unsubscribe(String topic, MessageSubscriber<OutT> subscriber) {
        if (subscriber == null) {
            throw new IllegalArgumentException("Subscriber cannot be null");
        }

        if (topic == null) {
            throw new IllegalArgumentException("Topic cannot be null");
        }

        Set<MessageSubscriber<OutT>> subscribers = topicSubscribers.get(topic);
        if (subscribers != null) {
            boolean removed = subscribers.remove(subscriber);
            if (subscribers.isEmpty()) {
                topicSubscribers.remove(topic);
            }
            if (removed) {
                metrics.recordSubscriberRemoved(topic);
            }
            return removed;
        }
        return false;
    }

    @Override
    public void close() {
        closed.set(true);
        topicSubscribers.clear();
    }

}
