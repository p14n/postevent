package com.p14n.postevent.broker;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DefaultMessageBroker<InT, OutT> implements MessageBroker<InT, OutT>, AutoCloseable {

    protected final ConcurrentHashMap<String, Set<MessageSubscriber<OutT>>> topicSubscribers = new ConcurrentHashMap<>();
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    private final AsyncExecutor asyncExecutor;

    public DefaultMessageBroker() {
        this(new DefaultExecutor(2));
    }

    public DefaultMessageBroker(AsyncExecutor asyncExecutor) {
        this.asyncExecutor = asyncExecutor;
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

        // Deliver to all subscribers for this topic
        Set<MessageSubscriber<OutT>> subscribers = topicSubscribers.get(topic);
        if (subscribers != null) {
            for (MessageSubscriber<OutT> subscriber : subscribers) {
                asyncExecutor.submit(() -> {
                    try {
                        subscriber.onMessage(convert(message));
                        return null;
                    } catch (Exception e) {
                        try {
                            subscriber.onError(e);
                        } catch (Exception ignored) {
                            // If error handling fails, we ignore it to protect other subscribers
                        }
                    }
                    return null;
                });
            }
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

        return topicSubscribers
                .computeIfAbsent(topic, k -> new CopyOnWriteArraySet<>())
                .add(subscriber);
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
