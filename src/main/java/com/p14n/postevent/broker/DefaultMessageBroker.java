package com.p14n.postevent.broker;

import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DefaultMessageBroker<InT, OutT> implements MessageBroker<InT, OutT>, AutoCloseable {

    protected final CopyOnWriteArraySet<MessageSubscriber<OutT>> subscribers = new CopyOnWriteArraySet<>();
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    private final AsyncExecutor asyncExecutor;

    public DefaultMessageBroker() {
        this(new DefaultExecutor(2));
    }

    public DefaultMessageBroker(AsyncExecutor asyncExecutor) {
        this.asyncExecutor = asyncExecutor;
    }

    protected boolean canProcess(InT message) {
        if (closed.get()) {
            throw new IllegalStateException("Broker is closed");
        }

        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        // If no subscribers, message is silently dropped
        if (subscribers.isEmpty()) {
            return false;
        }
        return true;

    }

    @Override
    public void publish(InT message) {

        if (!canProcess(message)) {
            return;
        }
        // Deliver to all subscribers
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

    @Override
    public boolean subscribe(MessageSubscriber<OutT> subscriber) {
        if (closed.get()) {
            throw new IllegalStateException("Broker is closed");
        }

        if (subscriber == null) {
            throw new IllegalArgumentException("Subscriber cannot be null");
        }

        return subscribers.add(subscriber);
    }

    @Override
    public boolean unsubscribe(MessageSubscriber<OutT> subscriber) {
        if (subscriber == null) {
            throw new IllegalArgumentException("Subscriber cannot be null");
        }

        return subscribers.remove(subscriber);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Notify all subscribers of shutdown
            Throwable shutdownError = new IllegalStateException("Message broker is shutting down");
            for (MessageSubscriber<OutT> subscriber : subscribers) {
                try {
                    subscriber.onError(shutdownError);
                } catch (Exception ignored) {
                    // Ignore errors during shutdown notification
                }
            }
            subscribers.clear();
        }
    }
}
