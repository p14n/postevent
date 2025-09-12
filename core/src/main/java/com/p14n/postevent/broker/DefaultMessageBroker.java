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

/**
 * Abstract base implementation of a message broker that supports
 * publish-subscribe messaging patterns.
 * Provides thread-safe handling of message publishing and subscription
 * management with telemetry support.
 *
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Thread-safe message publishing and subscription management</li>
 * <li>Asynchronous message delivery to subscribers</li>
 * <li>OpenTelemetry integration for metrics and tracing</li>
 * <li>Automatic resource cleanup via AutoCloseable</li>
 * </ul>
 *
 * <p>
 * The broker maintains a concurrent map of topic subscribers and ensures
 * thread-safe operations
 * for publishing messages and managing subscriptions. Messages are delivered
 * asynchronously to
 * subscribers using the provided {@link AsyncExecutor}.
 * </p>
 *
 * @param <InT>  The input message type, must implement {@link Traceable}
 * @param <OutT> The output message type delivered to subscribers
 */
public abstract class DefaultMessageBroker<InT extends Traceable, OutT>
        implements MessageBroker<InT, OutT>, AutoCloseable {

    /**
     * Thread-safe map storing topic subscribers.
     * Key is the topic name, value is a thread-safe set of subscribers.
     */
    protected final ConcurrentHashMap<String, Set<MessageSubscriber<OutT>>> topicSubscribers = new ConcurrentHashMap<>();

    /**
     * Flag indicating if the broker has been closed.
     */
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Executor for handling asynchronous message delivery.
     */
    private final AsyncExecutor asyncExecutor;

    /**
     * Metrics collector for broker operations.
     */
    protected final BrokerMetrics metrics;

    /**
     * OpenTelemetry tracer for distributed tracing.
     */
    protected final Tracer tracer;

    /**
     * OpenTelemetry instance for metrics and tracing.
     */
    protected final OpenTelemetry openTelemetry;

    /**
     * Creates a new broker instance with default executor configuration.
     *
     * @param ot        The OpenTelemetry instance for metrics and tracing
     * @param scopeName The scope name for OpenTelemetry instrumentation
     */
    public DefaultMessageBroker(OpenTelemetry ot, String scopeName) {
        this(new DefaultExecutor(2), ot, scopeName);
    }

    /**
     * Creates a new broker instance with custom executor configuration.
     *
     * @param asyncExecutor The executor for handling asynchronous message delivery
     * @param ot            The OpenTelemetry instance for metrics and tracing
     * @param scopeName     The scope name for OpenTelemetry instrumentation
     */
    public DefaultMessageBroker(AsyncExecutor asyncExecutor, OpenTelemetry ot, String scopeName) {
        this.asyncExecutor = asyncExecutor;
        this.metrics = new BrokerMetrics(ot.getMeter(scopeName));
        this.tracer = ot.getTracer(scopeName);
        this.openTelemetry = ot;
    }

    /**
     * Checks if a message can be processed for a given topic.
     * Validates broker state and message parameters.
     *
     * @param topic   The topic to check
     * @param message The message to validate
     * @return true if the message can be processed, false otherwise
     * @throws IllegalStateException    if the broker is closed
     * @throws IllegalArgumentException if topic or message is null
     */
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

        return topicSubscribers.containsKey(topic) && !topicSubscribers.get(topic).isEmpty();
    }

    /**
     * Converts the input message type to the output message type.
     * Must be implemented by concrete classes.
     *
     * @param message The input message to convert
     * @return The converted output message
     */
    public abstract OutT convert(InT message);

    /**
     * Publishes a message to all subscribers of the specified topic.
     * Messages are delivered asynchronously to all subscribers.
     * If a subscriber throws an exception during message processing,
     * its error handler is invoked and the exception is propagated.
     *
     * @param topic   The topic to publish to
     * @param message The message to publish
     */
    @Override
    public void publish(String topic, InT message) {
        if (!canProcess(topic, message)) {
            return;
        }

        metrics.recordPublished(topic);

        Set<MessageSubscriber<OutT>> subscribers = topicSubscribers.get(topic);
        if (subscribers != null) {
            processWithTelemetry(openTelemetry, tracer, message, "publish_message", () -> {
                for (MessageSubscriber<OutT> subscriber : subscribers) {
                    asyncExecutor.submit(() -> processWithTelemetry(openTelemetry, tracer, message, "process_message",
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

    /**
     * Subscribes a message handler to a specific topic.
     * Thread-safe subscription management using ConcurrentHashMap and
     * CopyOnWriteArraySet.
     *
     * @param topic      The topic to subscribe to
     * @param subscriber The subscriber to add
     * @return true if the subscription was added, false if it already existed
     * @throws IllegalStateException    if the broker is closed
     * @throws IllegalArgumentException if topic or subscriber is null
     */
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

    /**
     * Unsubscribes a message handler from a specific topic.
     * Thread-safe unsubscription with automatic topic cleanup when the last
     * subscriber is removed.
     *
     * @param topic      The topic to unsubscribe from
     * @param subscriber The subscriber to remove
     * @return true if the subscription was removed, false if it didn't exist
     * @throws IllegalArgumentException if topic or subscriber is null
     */
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

    /**
     * Closes the broker and removes all subscribers.
     * After closing, no new subscriptions can be added and no messages can be
     * published.
     */
    @Override
    public void close() {
        closed.set(true);
        topicSubscribers.clear();
    }
}
