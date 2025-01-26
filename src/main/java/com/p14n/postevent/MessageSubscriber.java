package com.p14n.postevent;

/**
 * Interface for message subscribers that can receive messages and error notifications.
 * @param <T> The type of messages this subscriber handles
 */
public interface MessageSubscriber<T> {
    
    /**
     * Called when a new message is available for processing.
     * @param message The message to process
     */
    void onMessage(T message);
    
    /**
     * Called when an error occurs that prevents further message processing.
     * @param error The error that occurred
     */
    void onError(Throwable error);
}
