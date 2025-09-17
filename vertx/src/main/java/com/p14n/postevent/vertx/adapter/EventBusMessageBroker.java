package com.p14n.postevent.vertx.adapter;

import com.p14n.postevent.Publisher;
import com.p14n.postevent.broker.AsyncExecutor;
import com.p14n.postevent.broker.EventMessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.vertx.codec.EventCodec;
import com.p14n.postevent.data.Event;
import io.opentelemetry.api.OpenTelemetry;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A message broker implementation that bridges the core EventMessageBroker
 * with Vert.x EventBus for reactive, asynchronous event processing.
 * 
 * <p>
 * This broker provides a dual-write pattern:
 * <ol>
 * <li>Events are first persisted to the database using the existing
 * Publisher</li>
 * <li>Events are then published to the Vert.x EventBus for real-time
 * distribution</li>
 * </ol>
 * </p>
 * 
 * <p>
 * Subscribers receive events from the EventBus, providing low-latency
 * event delivery while maintaining persistence guarantees.
 * </p>
 * 
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * Vertx vertx = Vertx.vertx();
 * DataSource dataSource = // configure datasource
 * AsyncExecutor executor = new DefaultExecutor();
 * 
 * EventBusMessageBroker broker = new EventBusMessageBroker(
 *     vertx, dataSource, executor, OpenTelemetry.noop(), "my-broker");
 * 
 * // Subscribe to events
 * broker.subscribe("orders", event -> {
 *     System.out.println("Received: " + event);
 * });
 * 
 * // Publish events (persisted + real-time)
 * Event event = Event.create(...);
 * broker.publish("orders", event);
 * }</pre>
 */
public class EventBusMessageBroker extends EventMessageBroker {
    private static final Logger logger = LoggerFactory.getLogger(EventBusMessageBroker.class);
    private final EventBus eventBus;
    private final DataSource dataSource;
    private final Map<String, MessageConsumer<Event>> consumers = new ConcurrentHashMap<>();

    /**
     * Creates a new EventBusMessageBroker.
     *
     * @param eventBus   The Vert.x EventBus instance to use
     * @param dataSource The DataSource for database persistence
     * @param executor   The AsyncExecutor for handling asynchronous operations
     * @param ot         OpenTelemetry instance for observability
     * @param name       Name identifier for this broker instance
     */
    public EventBusMessageBroker(EventBus eventBus, DataSource dataSource, AsyncExecutor executor,
            OpenTelemetry ot, String name) {
        super(executor, ot, name);
        this.eventBus = eventBus;
        this.dataSource = dataSource;

        // Register the Event codec for EventBus serialization
        eventBus.registerDefaultCodec(Event.class, new EventCodec());

        logger.atInfo()
                .addArgument(name)
                .log("EventBusMessageBroker initialized: {}");
    }


    /**
     * Publishes an event using the dual-write pattern.
     * The event is first persisted to the database, then published to the EventBus.
     *
     * @param topic The topic to publish to
     * @param event The event to publish
     */
    @Override
    public void publish(String topic, Event event) {
        logger.atDebug()
                .addArgument(topic)
                .addArgument(event.id())
                .log("Publishing event to topic {} with id {}");

        try {
            // First, persist to database using existing Publisher
            Publisher.publish(event, dataSource, topic);

            // Then, publish to EventBus for real-time distribution
            String eventBusAddress = "events." + topic;
            eventBus.publish(eventBusAddress, event);

            logger.atDebug()
                    .addArgument(topic)
                    .addArgument(event.id())
                    .log("Successfully published event to topic {} with id {}");

        } catch (Exception e) {
            logger.atError()
                    .addArgument(topic)
                    .addArgument(event.id())
                    .setCause(e)
                    .log("Failed to publish event to topic {} with id {}");
            throw new RuntimeException("Failed to publish event", e);
        }
    }

    /**
     * Subscribes to events on a specific topic via the EventBus.
     * Creates a consumer that listens to the EventBus address for the topic.
     *
     * @param topic      The topic to subscribe to
     * @param subscriber The subscriber that will receive events
     */
    public void subscribeToEventBus(String topic, MessageSubscriber<Event> subscriber) {
        logger.atInfo()
                .addArgument(topic)
                .log("Subscribing to topic: {}");

        String eventBusAddress = "events." + topic;

        MessageConsumer<Event> consumer = eventBus.consumer(eventBusAddress);
        consumer.handler(message -> {
            Event event = message.body();
            logger.atDebug()
                    .addArgument(topic)
                    .addArgument(event.id())
                    .log("Received event on topic {} with id {}");

            try {
                subscriber.onMessage(event);
            } catch (Exception e) {
                logger.atError()
                        .addArgument(topic)
                        .addArgument(event.id())
                        .setCause(e)
                        .log("Error processing event on topic {} with id {}");
            }
        });

        // Store consumer for potential cleanup
        consumers.put(topic, consumer);

        logger.atInfo()
                .addArgument(topic)
                .addArgument(eventBusAddress)
                .log("Successfully subscribed to topic {} at address {}");
    }

    /**
     * Unsubscribes from a topic by removing the EventBus consumer.
     *
     * @param topic The topic to unsubscribe from
     */
    public void unsubscribe(String topic) {
        MessageConsumer<Event> consumer = consumers.remove(topic);
        if (consumer != null) {
            consumer.unregister();
            logger.atInfo()
                    .addArgument(topic)
                    .log("Unsubscribed from topic: {}");
        }
    }

    /**
     * Closes the broker and cleans up all subscriptions.
     */
    @Override
    public void close() {
        logger.atInfo().log("Closing EventBusMessageBroker");

        // Unregister all consumers
        consumers.values().forEach(MessageConsumer::unregister);
        consumers.clear();

        super.close();

        logger.atInfo().log("EventBusMessageBroker closed");
    }
}
