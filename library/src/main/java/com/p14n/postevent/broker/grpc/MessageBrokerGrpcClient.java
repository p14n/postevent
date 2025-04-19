package com.p14n.postevent.broker.grpc;

import com.p14n.postevent.broker.EventMessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
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

public class MessageBrokerGrpcClient extends EventMessageBroker {
    private static final Logger logger = LoggerFactory.getLogger(MessageBrokerGrpcClient.class);

    private final MessageBrokerServiceGrpc.MessageBrokerServiceStub asyncStub;
    private final Set<String> subscribed = ConcurrentHashMap.newKeySet();;

    ManagedChannel channel;

    public MessageBrokerGrpcClient(OpenTelemetry ot, String host, int port) {
        this(ot, ManagedChannelBuilder.forAddress(host, port)
                .keepAliveTime(1, TimeUnit.HOURS)
                .keepAliveTimeout(30, TimeUnit.SECONDS)
                .usePlaintext()
                .build());
    }

    public MessageBrokerGrpcClient(OpenTelemetry ot, ManagedChannel channel) {
        super(ot);
        this.channel = channel;
        this.asyncStub = MessageBrokerServiceGrpc.newStub(channel);
    }

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
                subscribed.remove(topic);
            }

            @Override
            public void onCompleted() {
                logger.atInfo().log("Stream completed");
                subscribed.remove(topic);
            }
        };

        asyncStub.subscribeToEvents(request, responseObserver);
        subscribed.add(topic);
        // Send the subscription request

    }

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

    @Override
    public void publish(String topic, Event message) {
        super.publish(topic, message);
    }

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
