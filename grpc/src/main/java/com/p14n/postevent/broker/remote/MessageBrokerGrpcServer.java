package com.p14n.postevent.broker.remote;

import com.p14n.postevent.broker.grpc.EventResponse;
import com.p14n.postevent.broker.grpc.MessageBrokerServiceGrpc;
import com.p14n.postevent.broker.grpc.SubscriptionRequest;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A gRPC server implementation that exposes a MessageBroker service for remote
 * event subscription.
 * This class extends the auto-generated
 * MessageBrokerServiceGrpc.MessageBrokerServiceImplBase
 * to provide streaming event delivery to remote clients.
 *
 * <p>
 * Key features:
 * <ul>
 * <li>Streaming event subscription via gRPC</li>
 * <li>Thread-safe event delivery</li>
 * <li>Automatic cleanup of cancelled subscriptions</li>
 * <li>Error handling and propagation to clients</li>
 * <li>Structured logging for monitoring and debugging</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * MessageBroker<Event, Event> broker = // initialize broker
 * MessageBrokerGrpcServer server = new MessageBrokerGrpcServer(broker);
 * 
 * // Add to gRPC server
 * Server grpcServer = ServerBuilder.forPort(8080)
 *     .addService(server)
 *     .build();
 * grpcServer.start();
 * }</pre>
 */
public class MessageBrokerGrpcServer extends MessageBrokerServiceGrpc.MessageBrokerServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(MessageBrokerGrpcServer.class);
    private final MessageBroker<Event, Event> messageBroker;

    /**
     * Creates a new MessageBrokerGrpcServer with the specified message broker.
     *
     * @param messageBroker The message broker that will handle event distribution
     */
    public MessageBrokerGrpcServer(MessageBroker<Event, Event> messageBroker) {
        this.messageBroker = messageBroker;
    }

    /**
     * Sends an error response to the client through the gRPC stream.
     *
     * @param responseObserver The stream observer to send the error to
     * @param msg              The error message
     * @param error            The underlying error cause
     */
    private void errorResponse(StreamObserver<EventResponse> responseObserver, String msg,
            Throwable error) {
        responseObserver.onError(Status.INTERNAL
                .withDescription(msg)
                .withCause(error)
                .asRuntimeException());
    }

    /**
     * Handles subscription requests from clients and establishes a streaming
     * connection
     * for event delivery. This method implements the gRPC service endpoint for
     * event
     * subscription.
     *
     * <p>
     * The method:
     * <ul>
     * <li>Validates the subscription request</li>
     * <li>Sets up a message subscriber for the requested topic</li>
     * <li>Handles client disconnection and cleanup</li>
     * <li>Manages error conditions and client notification</li>
     * </ul>
     *
     * @param request          The subscription request containing the topic to
     *                         subscribe to
     * @param responseObserver The stream observer for sending events to the client
     */
    @Override
    public void subscribeToEvents(SubscriptionRequest request, StreamObserver<EventResponse> responseObserver) {
        String topic = request.getTopic();
        logger.atInfo().log("Subscription request received for topic: {}", topic);

        if (topic == null || topic.isEmpty()) {
            logger.atError().log("Invalid topic name received");
            errorResponse(responseObserver, "Topic name cannot be empty",
                    new IllegalArgumentException("Topic name cannot be empty"));
            return;
        }

        AtomicBoolean cancelled = new AtomicBoolean(false);

        try {
            MessageSubscriber<Event> subscriber = new MessageSubscriber<Event>() {
                @Override
                public void onMessage(Event event) {
                    logger.atDebug().log("Received message for topic: {}", topic);
                    if (cancelled.get()) {
                        return;
                    }
                    synchronized (responseObserver) {
                        try {
                            EventResponse response = convertToGrpcEvent(event);
                            responseObserver.onNext(response);
                        } catch (Exception e) {
                            logger.atError().setCause(e).log("Error sending event to client");
                            if (!cancelled.getAndSet(true)) {
                                errorResponse(responseObserver, "Error processing event", e);
                            }
                        }
                    }
                }

                @Override
                public void onError(Throwable error) {
                    logger.atError()
                            .addArgument(topic)
                            .setCause(error)
                            .log("Error subscribing to topic: {}");
                    if (!cancelled.getAndSet(true)) {
                        errorResponse(responseObserver, "Failed to subscribe to topic: " + topic, error);
                    }
                }
            };

            ServerCallStreamObserver<EventResponse> responseCallObserver = (ServerCallStreamObserver<EventResponse>) responseObserver;
            responseCallObserver.setOnCancelHandler(() -> {
                cancelled.set(true);
                messageBroker.unsubscribe(topic, subscriber);
                logger.atInfo().log("Unsubscribed from topic: {}", topic);
            });
            messageBroker.subscribe(topic, subscriber);

            logger.atInfo().log("Subscribed to topic: {}", topic);
        } catch (Exception e) {
            logger.atError().setCause(e).log("Error setting up subscription to topic: {}", topic);
            if (!cancelled.getAndSet(true)) {
                errorResponse(responseObserver, "Failed to subscribe to topic: " + topic, e);
            }
        }
    }

    /**
     * Converts an internal Event object to a gRPC EventResponse message.
     * Handles all optional fields and ensures proper conversion of data types.
     *
     * @param event The internal event to convert
     * @return The gRPC event response message
     */
    private EventResponse convertToGrpcEvent(Event event) {
        EventResponse.Builder builder = EventResponse.newBuilder()
                .setId(event.id())
                .setSource(event.source())
                .setType(event.type())
                .setIdn(event.idn());

        if (event.datacontenttype() != null) {
            builder.setDataContentType(event.datacontenttype());
        }

        if (event.dataschema() != null) {
            builder.setDataSchema(event.dataschema());
        }

        if (event.time() != null) {
            builder.setTime(event.time().toString());
        }

        if (event.subject() != null) {
            builder.setSubject(event.subject());
        }

        if (event.data() != null) {
            builder.setData(com.google.protobuf.ByteString.copyFrom(event.data()));
        }

        if (event.topic() != null) {
            builder.setTopic(event.topic());
        }

        if (event.traceparent() != null) {
            builder.setTraceparent(event.traceparent());
        }

        return builder.build();
    }
}
