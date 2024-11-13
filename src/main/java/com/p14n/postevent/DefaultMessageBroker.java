package com.p14n.postevent;

import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default implementation of MessageBroker using thread-safe collections.
 * @param <T> The type of messages this broker handles
 */
public class DefaultMessageBroker<T> implements MessageBroker<T> {
    
    private final CopyOnWriteArraySet<MessageSubscriber<T>> subscribers = new CopyOnWriteArraySet<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    @Override
    public void publish(T message) {
        if (closed.get()) {
            throw new IllegalStateException("Broker is closed");
        }
        
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        
        // If no subscribers, message is silently dropped
        if (subscribers.isEmpty()) {
            return;
        }
        
        // Deliver to all subscribers
        for (MessageSubscriber<T> subscriber : subscribers) {
            try {
                subscriber.onMessage(message);
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
    public boolean subscribe(MessageSubscriber<T> subscriber) {
        if (closed.get()) {
            throw new IllegalStateException("Broker is closed");
        }
        
        if (subscriber == null) {
            throw new IllegalArgumentException("Subscriber cannot be null");
        }
        
        return subscribers.add(subscriber);
    }
    
    @Override
    public boolean unsubscribe(MessageSubscriber<T> subscriber) {
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
            for (MessageSubscriber<T> subscriber : subscribers) {
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
