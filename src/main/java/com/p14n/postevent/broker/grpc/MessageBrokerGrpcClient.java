package com.p14n.postevent.broker.grpc;

import com.p14n.postevent.broker.DefaultMessageBroker;
import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.data.Event;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.time.OffsetDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MessageBrokerGrpcClient extends DefaultMessageBroker<Event> implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(MessageBrokerGrpcClient.class.getName());
    private final MessageBrokerServiceGrpc.MessageBrokerServiceStub asyncStub;
    private final AtomicBoolean subscribed = new AtomicBoolean(false);

    ManagedChannel channel;

    public MessageBrokerGrpcClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .keepAliveTime(1, TimeUnit.HOURS)
                .keepAliveTimeout(30, TimeUnit.SECONDS)
                .usePlaintext()
                .build());
    }

    public MessageBrokerGrpcClient(ManagedChannel channel) {
        this.channel = channel;
        this.asyncStub = MessageBrokerServiceGrpc.newStub(channel);
    }

    public void subscribeToEvents() {
        SubscriptionRequest request = SubscriptionRequest.newBuilder()
                .setTopic("*")
                .build();

        StreamObserver<EventResponse> responseObserver = new StreamObserver<EventResponse>() {
            @Override
            public void onNext(EventResponse response) {
                try {
                    LOGGER.log(Level.INFO, "Received event: " + response.getId());
                    Event event = convertFromGrpcEvent(response);
                    publish(event);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error processing event", e);
                }
            }

            @Override
            public void onError(Throwable t) {
                LOGGER.log(Level.SEVERE, "Error in event stream", t);
                subscribed.set(false);
            }

            @Override
            public void onCompleted() {
                LOGGER.info("Stream completed");
                subscribed.set(false);
            }
        };

        asyncStub.subscribeToEvents(request, responseObserver);
        subscribed.set(true);
        // Send the subscription request

    }

    private Event convertFromGrpcEvent(EventResponse grpcEvent) {
        OffsetDateTime time = null;
        if (!grpcEvent.getTime().isEmpty()) {
            time = OffsetDateTime.parse(grpcEvent.getTime());
        }

        return new Event(
                grpcEvent.getId(),
                grpcEvent.getSource(),
                grpcEvent.getType(),
                grpcEvent.getDataContentType(),
                grpcEvent.getDataSchema(),
                grpcEvent.getSubject(),
                grpcEvent.getData().toByteArray(),
                time.toInstant(),
                grpcEvent.getIdn());
    }

    @Override
    public void publish(Event message) {
        super.publish(message);
    }

    @Override
    public boolean subscribe(MessageSubscriber<Event> subscriber) {
        if (super.subscribe(subscriber)) {
            if (!subscribed.get()) {
                subscribeToEvents();
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean unsubscribe(MessageSubscriber<Event> subscriber) {
        boolean unsubscribed = super.unsubscribe(subscriber);
        if (subscribers.isEmpty()) {
            subscribed.set(false);
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