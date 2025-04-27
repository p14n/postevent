package com.p14n.postevent.broker.remote;

import com.p14n.postevent.broker.AsyncExecutor;
import com.p14n.postevent.broker.EventMessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.broker.grpc.EventResponse;
import com.p14n.postevent.broker.grpc.MessageBrokerServiceGrpc;
import com.p14n.postevent.broker.grpc.SubscriptionRequest;
import com.p14n.postevent.data.Event;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.OpenTelemetry;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A gRPC client implementation of the EventMessageBroker that connects to a
 * remote message broker service.
 * Provides streaming event subscription and publication capabilities over gRPC.
 *
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Streaming event subscription via gRPC</li>
 * <li>Automatic reconnection handling</li>
 * <li>Thread-safe subscription management</li>
 * <li>OpenTelemetry integration for observability</li>
 * </ul>
 */
public class MessageBrokerGrpcClient extends EventMessageBroker {
    private static final Logger logger = LoggerFactory.getLogger(MessageBrokerGrpcClient.class);

    private final MessageBrokerServiceGrpc.MessageBrokerServiceStub asyncStub;
    private final Set<String> subscribed = ConcurrentHashMap.newKeySet();

    ManagedChannel channel;

    /**
     * Creates a new MessageBrokerGrpcClient with the specified host and port.
     *
     * @param asyncExecutor Executor for handling asynchronous operations
     * @param ot            OpenTelemetry instance for monitoring
     * @param host          Remote broker host address
     * @param port          Remote broker port
     */
    public MessageBrokerGrpcClient(AsyncExecutor asyncExecutor, OpenTelemetry ot, String host, int port) {
        this(asyncExecutor, ot, ManagedChannelBuilder.forAddress(host, port)
                .keepAliveTime(1, TimeUnit.HOURS)
                .keepAliveTimeout(30, TimeUnit.SECONDS)
                .usePlaintext()
                .build());
    }

    /**
     * Creates a new MessageBrokerGrpcClient with a pre-configured gRPC channel.
     *
     * @param asyncExecutor Executor for handling asynchronous operations
     * @param ot            OpenTelemetry instance for monitoring
     * @param channel       Pre-configured gRPC ManagedChannel
     */
    public MessageBrokerGrpcClient(AsyncExecutor asyncExecutor, OpenTelemetry ot, ManagedChannel channel) {
        super(asyncExecutor, ot, "grpc_client_broker");
        this.channel = channel;
        this.asyncStub = MessageBrokerServiceGrpc.newStub(channel);
    }

    /**
     * Establishes a streaming subscription to events on the specified topic.
     * Creates a gRPC stream observer to handle incoming events and errors.
     *
     * @param topic Topic to subscribe to
     */
    public void subscribeToEvents(String topic) {
        SubscriptionRequest request = SubscriptionRequest.newBuilder()
                .setTopic(topic)
                .build();

        StreamObserver<EventResponse> responseObserver = new StreamObserver<EventResponse>() {
            @Override
            public void onNext(EventResponse response) {
                try {
                    logger.atDebug().log(() -> "Received event: " + response.getId());
                    Event event = convertFromGrpcEvent(response);
                    publish(topic, event);
                } catch (Exception e) {
                    logger.atError().setCause(e).log("Error processing event");
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.atError().setCause(t).log("Error in event stream");
            }

            @Override
            public void onCompleted() {
                logger.atInfo().log("Stream completed");
                subscribed.remove(topic);
            }
        };

        asyncStub.subscribeToEvents(request, responseObserver);
        subscribed.add(topic);
    }

    /**
     * Converts a gRPC event response into an internal Event object.
     *
     * @param grpcEvent The gRPC event response to convert
     * @return Converted Event object
     */
    private Event convertFromGrpcEvent(EventResponse grpcEvent) {
        OffsetDateTime time = null;
        if (!grpcEvent.getTime().isEmpty()) {
            time = OffsetDateTime.parse(grpcEvent.getTime());
        }
        return Event.create(
                grpcEvent.getId(),
                grpcEvent.getSource(),
                grpcEvent.getType(),
                grpcEvent.getDataContentType(),
                grpcEvent.getDataSchema(),
                grpcEvent.getSubject(),
                grpcEvent.getData().toByteArray(),
                time == null ? Instant.now() : time.toInstant(),
                grpcEvent.getIdn(),
                grpcEvent.getTopic(),
                grpcEvent.getTraceparent());
    }

    /**
     * {@inheritDoc}
     * Publishes an event to the specified topic.
     */
    @Override
    public void publish(String topic, Event message) {
        super.publish(topic, message);
    }

    /**
     * {@inheritDoc}
     * Subscribes to events on the specified topic, establishing a gRPC stream if
     * needed.
     *
     * @param topic      Topic to subscribe to
     * @param subscriber Subscriber to receive events
     * @return true if subscription was successful, false otherwise
     */
    @Override
    public boolean subscribe(String topic, MessageSubscriber<Event> subscriber) {
        if (super.subscribe(topic, subscriber)) {
            if (!subscribed.contains(topic)) {
                subscribeToEvents(topic);
            }
            return true;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     * Unsubscribes from events and manages gRPC channel cleanup.
     *
     * @param topic      Topic to unsubscribe from
     * @param subscriber Subscriber to remove
     * @return true if unsubscription was successful, false otherwise
     */
    @Override
    public boolean unsubscribe(String topic, MessageSubscriber<Event> subscriber) {
        boolean unsubscribed = super.unsubscribe(topic, subscriber);
        if (topicSubscribers.isEmpty()) {
            subscribed.remove(topic);
            this.channel.shutdownNow();
            try {
                this.channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            this.channel = null;
        }
        return unsubscribed;
    }
}
