package com.p14n.postevent.vertx.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.p14n.postevent.catchup.CatchupServerInterface;
import com.p14n.postevent.data.Event;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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


    private <R> R requestAndDecode(
            String address,
            JsonObject payload,
            Function<Object, R> decoder
    ) {
        try {
            CompletableFuture<Object> fut = new CompletableFuture<>();
            eventBus.request(address, payload).andThen( ar -> {
                if (ar.succeeded()) {
                    fut.complete(ar.result().body());
                } else {
                    fut.completeExceptionally(
                            new RuntimeException("Bus request failed: " + ar.cause().getMessage(), ar.cause())
                    );
                }
            });
            Object body = fut.get(timeoutSeconds, TimeUnit.SECONDS);
            return decoder.apply(body);
        } catch (Exception e) {
            throw new RuntimeException("Request to " + address + " failed", e);
        }
    }

    @Override
    public List<Event> fetchEvents(long fromId, long toId, int limit, String topic) {
        JsonObject req = new JsonObject()
                .put("fromId", fromId)
                .put("toId", toId)
                .put("limit", limit)
                .put("topic", topic);

        // decode the reply-body string into List<Event>
        return requestAndDecode(
                FETCH_EVENTS_ADDRESS + topic,
                req,
                body -> Json.decodeValue((String) body, List.class )
        );
    }

    @Override
    public long getLatestMessageId(String topic) {
        JsonObject req = new JsonObject().put("topic", topic);

        // extract "latestId" from the returned JsonObject
        return requestAndDecode(
                GET_LATEST_MESSAGE_ID_ADDRESS + topic,
                req,
                body -> ((JsonObject) body).getLong("latestId")
        );
    }
}
