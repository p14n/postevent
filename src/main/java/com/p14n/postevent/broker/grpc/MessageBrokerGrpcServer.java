package com.p14n.postevent.broker.grpc;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MessageBrokerGrpcServer extends MessageBrokerServiceGrpc.MessageBrokerServiceImplBase {
    private static final Logger LOGGER = Logger.getLogger(MessageBrokerGrpcServer.class.getName());
    private final MessageBroker<Event,Event> messageBroker;

    public MessageBrokerGrpcServer(MessageBroker<Event,Event> messageBroker) {
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
        LOGGER.info("Received subscription request for topic: " + topic);

        AtomicBoolean cancelled = new AtomicBoolean(false);

        try {
            // Create a subscription handler
            MessageSubscriber<Event> subscriber = new MessageSubscriber<Event>() {
                @Override
                public void onMessage(Event event) {
                    // Skip if the stream has been cancelled
                    if (cancelled.get()) {
                        return;
                    }
                    try {
                        // Convert Event to EventResponse
                        EventResponse response = convertToGrpcEvent(event);
                        responseObserver.onNext(response);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error sending event to client", e);
                        if (!cancelled.getAndSet(true)) {
                            errorResponse(responseObserver, "Error processing event", e);
                        }
                    }
                }

                @Override
                public void onError(Throwable error) {
                    LOGGER.log(Level.SEVERE, "Error subscribing to topic: " + topic, error);
                    if (!cancelled.getAndSet(true)) {
                        errorResponse(responseObserver, "Failed to subscribe to topic: " + topic, error);
                    }
                }

            };
            ServerCallStreamObserver<EventResponse> responseCallObserver = (ServerCallStreamObserver<EventResponse>) responseObserver;
            responseCallObserver.setOnCancelHandler(() -> {
                cancelled.set(true);
                messageBroker.unsubscribe(subscriber);
                LOGGER.log(Level.INFO, "Unsubscribed from topic: " + topic);
            });
            messageBroker.subscribe(subscriber);

            // Handle cancellation
            // responseObserver.onCompleted();
            LOGGER.log(Level.INFO, "Subscribed to topic: " + topic);

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error subscribing to topic: " + topic, e);
            if (!cancelled.getAndSet(true)) {
                errorResponse(responseObserver, "Failed to subscribe to topic: " + topic, e);
            }
        }
    }

    private EventResponse convertToGrpcEvent(Event event) {
        EventResponse.Builder builder = EventResponse.newBuilder()
                .setId(event.id())
                .setSource(event.source())
                .setType(event.type());

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

        builder.setIdn(event.idn());

        return builder.build();
    }
}