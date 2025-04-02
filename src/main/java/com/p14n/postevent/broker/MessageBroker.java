package com.p14n.postevent.broker;

/**
 * Thread-safe message broker interface for publishing messages and managing
 * subscribers.
 * 
 * @param <InT>  The type of messages this broker accepts
 * @param <OutT> The type of messages this broker delivers to subscribers
 */
public interface MessageBroker<InT, OutT> {

    /**
     * Publishes a message to all subscribers of the specified topic.
     * If no subscribers are present for the topic, the message is silently dropped.
     * 
     * @param topic   The topic to publish to
     * @param message The message to publish
     */
    void publish(String topic, InT message);

    /**
     * Adds a subscriber to receive messages for the specified topic.
     * 
     * @param topic      The topic to subscribe to
     * @param subscriber The subscriber to add
     * @return true if the subscriber was added, false if it was already present
     */
    boolean subscribe(String topic, MessageSubscriber<OutT> subscriber);

    /**
     * Removes a subscriber from receiving messages for the specified topic.
     * 
     * @param topic      The topic to unsubscribe from
     * @param subscriber The subscriber to remove
     * @return true if the subscriber was removed, false if it wasn't present
     */
    boolean unsubscribe(String topic, MessageSubscriber<OutT> subscriber);

    /**
     * Closes the broker and releases any resources.
     * After closing, no more messages can be published or subscribers added.
     */
    void close();

    /**
     * Converts an incoming message to the outgoing message type
     * 
     * @param m The message to convert
     * @return The converted message
     */
    OutT convert(InT m);
}
