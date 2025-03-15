package com.p14n.postevent.broker;

/**
 * Thread-safe message broker interface for publishing messages and managing subscribers.
 * @param <T> The type of messages this broker handles
 */
public interface MessageBroker<InT,OutT> {
    
    /**
     * Publishes a message to all current subscribers.
     * If no subscribers are present, the message is silently dropped.
     * @param message The message to publish
     */
    void publish(InT message);
    
    /**
     * Adds a subscriber to receive messages.
     * @param subscriber The subscriber to add
     * @return true if the subscriber was added, false if it was already present
     */
    boolean subscribe(MessageSubscriber<OutT> subscriber);
    
    /**
     * Removes a subscriber from receiving messages.
     * @param subscriber The subscriber to remove
     * @return true if the subscriber was removed, false if it wasn't present
     */
    boolean unsubscribe(MessageSubscriber<OutT> subscriber);
    
    /**
     * Closes the broker and releases any resources.
     * After closing, no more messages can be published or subscribers added.
     */
    void close();

    OutT convert(InT m);
}
