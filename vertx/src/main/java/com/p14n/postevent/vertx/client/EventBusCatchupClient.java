package com.p14n.postevent.vertx.client;

import com.p14n.postevent.catchup.CatchupServerInterface;
import com.p14n.postevent.data.Event;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.p14n.postevent.vertx.adapter.EventBusCatchupService.FETCH_EVENTS_ADDRESS;
import static com.p14n.postevent.vertx.adapter.EventBusCatchupService.GET_LATEST_MESSAGE_ID_ADDRESS;

/**
 * Client implementation of CatchupServerInterface that sends requests
 * over the Vert.x EventBus to a remote EventBusCatchupService.
 * 
 * <p>
 * This client provides a synchronous API that internally uses the
 * asynchronous EventBus request-reply pattern. All operations have
 * configurable timeouts to prevent indefinite blocking.
 * </p>
 * 
 * <p>
 * The client communicates with EventBusCatchupService using JSON-encoded
 * messages over the EventBus, making it suitable for both local and
 * distributed deployments.
 * </p>
 * 
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * EventBus eventBus = vertx.eventBus();
 * EventBusCatchupClient client = new EventBusCatchupClient(eventBus);
 * 
 * // Fetch events
 * List<Event> events = client.fetchEvents(100L, 200L, 50, "orders");
 * 
 * // Get latest message ID
 * long latestId = client.getLatestMessageId("orders");
 * }</pre>
 */
public class EventBusCatchupClient implements CatchupServerInterface {
    private static final Logger logger = LoggerFactory.getLogger(EventBusCatchupClient.class);

    private static final long DEFAULT_TIMEOUT_SECONDS = 30;

    private final EventBus eventBus;
    private final long timeoutSeconds;

    /**
     * Creates a new EventBusCatchupClient with default timeout.
     *
     * @param eventBus The Vert.x EventBus to use for communication
     */
    public EventBusCatchupClient(EventBus eventBus) {
        this(eventBus, DEFAULT_TIMEOUT_SECONDS);
    }

    /**
     * Creates a new EventBusCatchupClient with custom timeout.
     *
     * @param eventBus       The Vert.x EventBus to use for communication
     * @param timeoutSeconds Timeout in seconds for EventBus requests
     */
    public EventBusCatchupClient(EventBus eventBus, long timeoutSeconds) {
        this.eventBus = eventBus;
        this.timeoutSeconds = timeoutSeconds;
    }

    /**
     * Fetches events within the specified ID range for a topic.
     * Sends a request to the EventBusCatchupService and waits for the response.
     *
     * @param fromId The starting event ID (inclusive)
     * @param toId   The ending event ID (inclusive)
     * @param limit  Maximum number of events to return
     * @param topic  The topic to fetch events from
     * @return List of events within the specified range
     * @throws Exception If the request fails or times out
     */
    @Override
    public List<Event> fetchEvents(long fromId, long toId, int limit, String topic) {
        logger.atDebug()
                .addArgument(fromId)
                .addArgument(toId)
                .addArgument(limit)
                .addArgument(topic)
                .log("Fetching events: fromId={}, toId={}, limit={}, topic={}");

        JsonObject request = new JsonObject()
                .put("fromId", fromId)
                .put("toId", toId)
                .put("limit", limit)
                .put("topic", topic);

        try {
            CompletableFuture<String> future = new CompletableFuture<>();

            eventBus.request(FETCH_EVENTS_ADDRESS + topic, request, reply -> {
                if (reply.succeeded()) {
                    String eventsJson = (String) reply.result().body();
                    future.complete(eventsJson);
                } else {
                    future.completeExceptionally(new RuntimeException(
                            "Failed to fetch events: " + reply.cause().getMessage(), reply.cause()));
                }
            });

            String eventsJson = future.get(timeoutSeconds, TimeUnit.SECONDS);
            List<Event> events = Json.decodeValue(eventsJson, List.class);

            logger.atDebug()
                    .addArgument(events.size())
                    .addArgument(topic)
                    .log("Successfully fetched {} events for topic {}", events.size(), topic);

            return events;

        } catch (Exception e) {
            logger.atError()
                    .addArgument(topic)
                    .setCause(e)
                    .log("Error fetching events for topic {}", topic);
            throw new RuntimeException("Failed to fetch events for topic " + topic, e);
        }
    }

    /**
     * Gets the latest message ID for a specific topic.
     * Sends a request to the EventBusCatchupService and waits for the response.
     *
     * @param topic The topic to get the latest message ID for
     * @return The latest message ID for the topic
     * @throws Exception If the request fails or times out
     */
    @Override
    public long getLatestMessageId(String topic) {
        logger.atDebug()
                .addArgument(topic)
                .log("Getting latest message ID for topic: {}");

        JsonObject request = new JsonObject().put("topic", topic);

        try {
            CompletableFuture<JsonObject> future = new CompletableFuture<>();

            eventBus.request(GET_LATEST_MESSAGE_ID_ADDRESS + topic, request, reply -> {
                if (reply.succeeded()) {
                    JsonObject response = (JsonObject) reply.result().body();
                    future.complete(response);
                } else {
                    future.completeExceptionally(new RuntimeException(
                            "Failed to get latest message ID: " + reply.cause().getMessage(), reply.cause()));
                }
            });

            JsonObject response = future.get(timeoutSeconds, TimeUnit.SECONDS);
            long latestId = response.getLong("latestId");

            logger.atDebug()
                    .addArgument(latestId)
                    .addArgument(topic)
                    .log("Successfully retrieved latest message ID {} for topic {}", latestId, topic);

            return latestId;

        } catch (Exception e) {
            logger.atError()
                    .addArgument(topic)
                    .setCause(e)
                    .log("Error getting latest message ID for topic {}", topic);
            throw new RuntimeException("Failed to get latest message ID for topic " + topic, e);
        }
    }
}
