package com.p14n.postevent.broker;

import com.p14n.postevent.data.Traceable;

/**
 * Enumeration of system-level events used for internal coordination and
 * control.
 * Implements {@link Traceable} to support distributed tracing and event
 * correlation.
 *
 * <p>
 * Available events:
 * </p>
 * <ul>
 * <li>{@code CatchupRequired}: Signals that a topic needs to catch up on missed
 * events</li>
 * <li>{@code UnprocessedCheckRequired}: Signals that unprocessed events need to
 * be checked</li>
 * <li>{@code FetchLatest}: Signals that the latest events should be fetched for
 * a topic</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * SystemEvent event = SystemEvent.CatchupRequired.withTopic("orders");
 * broker.publish(event.topic(), event);
 * }</pre>
 */
public enum SystemEvent implements Traceable {

    /**
     * Signals that a topic needs to catch up on missed events.
     * Used by the {@link com.p14n.postevent.catchup.CatchupService} to initiate
     * catchup processing.
     */
    CatchupRequired,

    /**
     * Signals that unprocessed events need to be checked.
     * Used to trigger verification of events that may have been missed or failed
     * processing.
     */
    UnprocessedCheckRequired,

    /**
     * Signals that the latest events should be fetched for a topic.
     * Used to request immediate retrieval of recent events without full catchup.
     */
    FetchLatest;

    /**
     * The topic this event is associated with.
     */
    public String topic;

    /**
     * Associates a topic with this system event.
     *
     * @param topic the topic to associate with this event
     * @return this event instance with the topic set
     */
    public SystemEvent withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    @Override
    public String id() {
        return this.toString();
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String subject() {
        return "";
    }

    @Override
    public String traceparent() {
        return null;
    }
}
