package com.p14n.postevent.adapter;

import com.p14n.postevent.catchup.CatchupServerInterface;
import com.p14n.postevent.data.Event;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that exposes CatchupServerInterface methods via Vert.x EventBus messaging.
 * This allows remote clients to request catchup operations through the EventBus
 * using a request-reply pattern.
 * 
 * <p>
 * The service listens on specific EventBus addresses and delegates to the
 * underlying CatchupServerInterface implementation. All requests and responses
 * are JSON-encoded for simplicity and debugging.
 * </p>
 * 
 * <p>
 * EventBus addresses:
 * <ul>
 * <li>{@code catchup.fetchEvents} - Fetch events within a range</li>
 * <li>{@code catchup.getLatestMessageId} - Get the latest message ID for a topic</li>
 * </ul>
 * </p>
 * 
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * CatchupServerInterface catchupServer = new CatchupServer(dataSource);
 * EventBus eventBus = vertx.eventBus();
 * 
 * EventBusCatchupService service = new EventBusCatchupService(catchupServer, eventBus);
 * service.start();
 * 
 * // Service is now listening for catchup requests on the EventBus
 * }</pre>
 */
public class EventBusCatchupService {
    private static final Logger logger = LoggerFactory.getLogger(EventBusCatchupService.class);
    
    private static final String FETCH_EVENTS_ADDRESS = "catchup.fetchEvents";
    private static final String GET_LATEST_MESSAGE_ID_ADDRESS = "catchup.getLatestMessageId";
    
    private final CatchupServerInterface catchupServer;
    private final EventBus eventBus;
    private MessageConsumer<JsonObject> fetchEventsConsumer;
    private MessageConsumer<JsonObject> getLatestMessageIdConsumer;
    
    /**
     * Creates a new EventBusCatchupService.
     *
     * @param catchupServer The underlying catchup server implementation
     * @param eventBus      The Vert.x EventBus to use for messaging
     */
    public EventBusCatchupService(CatchupServerInterface catchupServer, EventBus eventBus) {
        this.catchupServer = catchupServer;
        this.eventBus = eventBus;
    }
    
    /**
     * Starts the service by registering EventBus consumers for catchup operations.
     * This method sets up listeners for both fetchEvents and getLatestMessageId requests.
     */
    public void start() {
        logger.atInfo().log("Starting EventBusCatchupService");
        
        // Register consumer for fetchEvents requests
        fetchEventsConsumer = eventBus.consumer(FETCH_EVENTS_ADDRESS, this::handleFetchEvents);
        
        // Register consumer for getLatestMessageId requests
        getLatestMessageIdConsumer = eventBus.consumer(GET_LATEST_MESSAGE_ID_ADDRESS, this::handleGetLatestMessageId);
        
        logger.atInfo()
            .addArgument(FETCH_EVENTS_ADDRESS)
            .addArgument(GET_LATEST_MESSAGE_ID_ADDRESS)
            .log("EventBusCatchupService started, listening on addresses: {} and {}");
    }
    
    /**
     * Stops the service by unregistering EventBus consumers.
     */
    public void stop() {
        logger.atInfo().log("Stopping EventBusCatchupService");
        
        if (fetchEventsConsumer != null) {
            fetchEventsConsumer.unregister();
            fetchEventsConsumer = null;
        }
        
        if (getLatestMessageIdConsumer != null) {
            getLatestMessageIdConsumer.unregister();
            getLatestMessageIdConsumer = null;
        }
        
        logger.atInfo().log("EventBusCatchupService stopped");
    }
    
    /**
     * Handles fetchEvents requests from the EventBus.
     * 
     * Expected request format:
     * <pre>{@code
     * {
     *   "fromId": 100,
     *   "toId": 200,
     *   "limit": 50,
     *   "topic": "orders"
     * }
     * }</pre>
     *
     * @param message The EventBus message containing the request
     */
    private void handleFetchEvents(Message<JsonObject> message) {
        JsonObject request = message.body();
        
        try {
            long fromId = request.getLong("fromId");
            long toId = request.getLong("toId");
            int limit = request.getInteger("limit");
            String topic = request.getString("topic");
            
            logger.atDebug()
                .addArgument(fromId)
                .addArgument(toId)
                .addArgument(limit)
                .addArgument(topic)
                .log("Handling fetchEvents request: fromId={}, toId={}, limit={}, topic={}");
            
            List<Event> events = catchupServer.fetchEvents(fromId, toId, limit, topic);
            
            // Serialize events to JSON and reply
            String eventsJson = Json.encode(events);
            message.reply(eventsJson);
            
            logger.atDebug()
                .addArgument(events.size())
                .addArgument(topic)
                .log("Successfully fetched {} events for topic {}", events.size(), topic);
            
        } catch (Exception e) {
            logger.atError()
                .setCause(e)
                .log("Error handling fetchEvents request");
            message.fail(500, e.getMessage());
        }
    }
    
    /**
     * Handles getLatestMessageId requests from the EventBus.
     * 
     * Expected request format:
     * <pre>{@code
     * {
     *   "topic": "orders"
     * }
     * }</pre>
     *
     * @param message The EventBus message containing the request
     */
    private void handleGetLatestMessageId(Message<JsonObject> message) {
        JsonObject request = message.body();
        
        try {
            String topic = request.getString("topic");
            
            logger.atDebug()
                .addArgument(topic)
                .log("Handling getLatestMessageId request for topic: {}");
            
            long latestId = catchupServer.getLatestMessageId(topic);
            
            // Create response with latest ID
            JsonObject response = new JsonObject().put("latestId", latestId);
            message.reply(response);
            
            logger.atDebug()
                .addArgument(latestId)
                .addArgument(topic)
                .log("Successfully retrieved latest message ID {} for topic {}", latestId, topic);
            
        } catch (Exception e) {
            logger.atError()
                .setCause(e)
                .log("Error handling getLatestMessageId request");
            message.fail(500, e.getMessage());
        }
    }
}
