package com.p14n.postevent.broker.grpc;

import com.p14n.postevent.broker.EventMessageBroker;
import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.catchup.CatchupServer;
import com.p14n.postevent.data.Event;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.time.OffsetDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageBrokerGrpcClient extends EventMessageBroker implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MessageBrokerGrpcClient.class);

    private final MessageBrokerServiceGrpc.MessageBrokerServiceStub asyncStub;
    private final AtomicBoolean subscribed = new AtomicBoolean(false);

    ManagedChannel channel;
    String topic;

    public MessageBrokerGrpcClient(String host, int port, String topic) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .keepAliveTime(1, TimeUnit.HOURS)
                .keepAliveTimeout(30, TimeUnit.SECONDS)
                .usePlaintext()
                .build(), topic);
    }

    public MessageBrokerGrpcClient(ManagedChannel channel, String topic) {
        this.channel = channel;
        this.asyncStub = MessageBrokerServiceGrpc.newStub(channel);
        this.topic = topic;
    }

    public void subscribeToEvents() {
        SubscriptionRequest request = SubscriptionRequest.newBuilder()
                .setTopic(topic)
                .build();

        StreamObserver<EventResponse> responseObserver = new StreamObserver<EventResponse>() {
            @Override
            public void onNext(EventResponse response) {
                try {
                    logger.atDebug().log(() -> "Received event: " + response.getId());
                    Event event = convertFromGrpcEvent(response);
                    publish(event);
                } catch (Exception e) {
                    logger.atError().setCause(e).log("Error processing event");
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.atError().setCause(t).log("Error in event stream");
                subscribed.set(false);
            }

            @Override
            public void onCompleted() {
                logger.atInfo().log("Stream completed");
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
        return Event.create(
                grpcEvent.getId(),
                grpcEvent.getSource(),
                grpcEvent.getType(),
                grpcEvent.getDataContentType(),
                grpcEvent.getDataSchema(),
                grpcEvent.getSubject(),
                grpcEvent.getData().toByteArray(),
                time.toInstant(),
                grpcEvent.getIdn(),
                grpcEvent.getTopic());
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
