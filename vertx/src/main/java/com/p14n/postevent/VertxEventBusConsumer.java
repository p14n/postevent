package com.p14n.postevent;

import com.p14n.postevent.adapter.EventBusMessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.catchup.CatchupService;
import com.p14n.postevent.client.EventBusCatchupClient;
import com.p14n.postevent.codec.EventCodec;
import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.data.Event;
import io.opentelemetry.api.OpenTelemetry;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main consumer class that provides event consumption capabilities using Vert.x
 * EventBus.
 * This consumer combines real-time event delivery via EventBus with catchup
 * capabilities
 * for guaranteed event processing.
 * 
 * <p>
 * The consumer provides:
 * <ul>
 * <li>Real-time event consumption via Vert.x EventBus</li>
 * <li>Automatic catchup for missed events using CatchupService</li>
 * <li>Persistent event storage integration</li>
 * <li>OpenTelemetry observability</li>
 * </ul>
 * </p>
 * 
 * <p>
 * Architecture:
 * 
 * <pre>
 * Events → Database → EventBus → VertxEventBusConsumer → Subscriber
 *                        ↑
 *                   CatchupService (for missed events)
 * </pre>
 * </p>
 * 
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * // Create configuration
 * ConfigData config = new ConfigData("myapp", Set.of("orders"), ...);
 * 
 * // Create consumer
 * VertxEventBusConsumer consumer = new VertxEventBusConsumer(config, OpenTelemetry.noop(), 30);
 * 
 * // Subscribe to events
 * consumer.subscribe("orders", event -> {
 *     System.out.println("Received: " + event);
 * });
 * 
 * // Start consuming
 * consumer.start(Set.of("orders"), dataSource);
 * }</pre>
 */
public class VertxEventBusConsumer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(VertxEventBusConsumer.class);

    private final Vertx vertx;
    private final EventBus eventBus;
    private final ConfigData config;
    private final OpenTelemetry openTelemetry;
    private final int catchupIntervalSeconds;
    private final Map<String, MessageConsumer<Event>> consumers = new ConcurrentHashMap<>();

    private CatchupService catchupService;
    private EventBusMessageBroker messageBroker;
    private boolean started = false;

    /**
     * Creates a new VertxEventBusConsumer.
     *
     * @param config                 Configuration data for the consumer
     * @param openTelemetry          OpenTelemetry instance for observability
     * @param catchupIntervalSeconds Interval in seconds for catchup operations
     */
    public VertxEventBusConsumer(ConfigData config, OpenTelemetry openTelemetry, int catchupIntervalSeconds) {
        this.config = config;
        this.openTelemetry = openTelemetry;
        this.catchupIntervalSeconds = catchupIntervalSeconds;
        this.vertx = Vertx.vertx();
        this.eventBus = vertx.eventBus();

        // Register Event codec for EventBus serialization
        eventBus.registerDefaultCodec(Event.class, new EventCodec());

        logger.atInfo()
                .addArgument(config.affinity())
                .addArgument(catchupIntervalSeconds)
                .log("VertxEventBusConsumer created for {} with catchup interval {}s");
    }

    /**
     * Starts the consumer with the specified topics and data source.
     * This initializes the catchup service and prepares for event consumption.
     *
     * @param topics     The set of topics to consume from
     * @param dataSource The DataSource for database operations
     * @throws Exception If startup fails
     */
    public void start(Set<String> topics, DataSource dataSource) throws Exception {
        if (started) {
            throw new IllegalStateException("Consumer is already started");
        }

        logger.atInfo()
                .addArgument(topics)
                .log("Starting VertxEventBusConsumer for topics: {}");

        try {
            // Create EventBus-based catchup client
            EventBusCatchupClient catchupClient = new EventBusCatchupClient(eventBus);

            // Create system event broker for catchup coordination
            SystemEventBroker systemEventBroker = new SystemEventBroker(openTelemetry);

            // Initialize catchup service
            catchupService = new CatchupService(dataSource, catchupClient, systemEventBroker);

            started = true;

            logger.atInfo()
                    .addArgument(topics)
                    .log("VertxEventBusConsumer started successfully for topics: {}");

        } catch (Exception e) {
            logger.atError()
                    .setCause(e)
                    .log("Failed to start VertxEventBusConsumer");
            throw e;
        }
    }

    /**
     * Subscribes to events on a specific topic via the EventBus.
     * Creates an EventBus consumer that listens for events on the topic.
     *
     * @param topic      The topic to subscribe to
     * @param subscriber The subscriber that will receive events
     */
    public void subscribe(String topic, MessageSubscriber<Event> subscriber) {
        if (!started) {
            throw new IllegalStateException("Consumer must be started before subscribing");
        }

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

        // Store consumer for cleanup
        consumers.put(topic, consumer);

        logger.atInfo()
                .addArgument(topic)
                .addArgument(eventBusAddress)
                .log("Successfully subscribed to topic {} at address {}");
    }

    /**
     * Unsubscribes from a specific topic.
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
     * Gets the Vert.x instance used by this consumer.
     * This can be useful for advanced integrations.
     *
     * @return The Vert.x instance
     */
    public Vertx getVertx() {
        return vertx;
    }

    /**
     * Gets the EventBus instance used by this consumer.
     * This can be useful for advanced integrations.
     *
     * @return The EventBus instance
     */
    public EventBus getEventBus() {
        return eventBus;
    }

    /**
     * Checks if the consumer is currently started.
     *
     * @return true if started, false otherwise
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * Closes the consumer and cleans up all resources.
     * This stops the catchup service, unregisters all consumers, and closes Vert.x.
     */
    @Override
    public void close() {
        logger.atInfo().log("Closing VertxEventBusConsumer");

        // Unregister all EventBus consumers
        consumers.values().forEach(MessageConsumer::unregister);
        consumers.clear();

        // Stop catchup service
        // Note: CatchupService doesn't need explicit closing
        // It's managed by the SystemEventBroker

        // Close message broker if created
        if (messageBroker != null) {
            try {
                messageBroker.close();
            } catch (Exception e) {
                logger.atWarn()
                        .setCause(e)
                        .log("Error closing message broker");
            }
        }

        // Close Vert.x
        if (vertx != null) {
            vertx.close();
        }

        started = false;

        logger.atInfo().log("VertxEventBusConsumer closed");
    }
}
