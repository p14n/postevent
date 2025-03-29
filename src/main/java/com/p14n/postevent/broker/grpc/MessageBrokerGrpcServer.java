package com.p14n.postevent.broker.grpc;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageBrokerGrpcServer extends MessageBrokerServiceGrpc.MessageBrokerServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(MessageBrokerGrpcServer.class);
    private final MessageBroker<Event, Event> messageBroker;

    public MessageBrokerGrpcServer(MessageBroker<Event, Event> messageBroker) {
        this.messageBroker = messageBroker;
    }

    private void errorResponse(StreamObserver<EventResponse> responseObserver, String msg,
            Throwable error) {
        responseObserver.onError(Status.INTERNAL
                .withDescription(msg)
                .withCause(error)
                .asRuntimeException());
    }

    @Override
    public void subscribeToEvents(SubscriptionRequest request, StreamObserver<EventResponse> responseObserver) {
        String topic = request.getTopic();
        logger.info("Subscription request received for topic: {}", topic);

        if (topic == null || topic.isEmpty()) {
            logger.error("Invalid topic name received");
            errorResponse(responseObserver, "Topic name cannot be empty",
                    new IllegalArgumentException("Topic name cannot be empty"));
            return;
        }

        AtomicBoolean cancelled = new AtomicBoolean(false);

        try {
            MessageSubscriber<Event> subscriber = new MessageSubscriber<Event>() {
                @Override
                public void onMessage(Event event) {
                    logger.info("Received message for topic: {}", topic);
                    if (cancelled.get()) {
                        return;
                    }
                    synchronized (responseObserver) {
                        try {
                            EventResponse response = convertToGrpcEvent(event);
                            responseObserver.onNext(response);
                        } catch (Exception e) {
                            logger.error("Error sending event to client", e);
                            if (!cancelled.getAndSet(true)) {
                                errorResponse(responseObserver, "Error processing event", e);
                            }
                        }
                    }
                }

                @Override
                public void onError(Throwable error) {
                    logger.error("Error subscribing to topic: {}", topic, error);
                    if (!cancelled.getAndSet(true)) {
                        errorResponse(responseObserver, "Failed to subscribe to topic: " + topic, error);
                    }
                }
            };

            ServerCallStreamObserver<EventResponse> responseCallObserver = (ServerCallStreamObserver<EventResponse>) responseObserver;
            responseCallObserver.setOnCancelHandler(() -> {
                cancelled.set(true);
                messageBroker.unsubscribe(subscriber);
                logger.info("Unsubscribed from topic: {}", topic);
            });
            messageBroker.subscribe(subscriber);

            logger.info("Subscribed to topic: {}", topic);
        } catch (Exception e) {
            logger.error("Error subscribing to topic: " + topic, e);
            if (!cancelled.getAndSet(true)) {
                errorResponse(responseObserver, "Failed to subscribe to topic: " + topic, e);
            }
        }
    }

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

        return builder.build();
    }
}
