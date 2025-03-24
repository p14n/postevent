package com.p14n.postevent.catchup.grpc;

import com.p14n.postevent.catchup.CatchupServerInterface;
import com.p14n.postevent.data.Event;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CatchupGrpcClient implements CatchupServerInterface, AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(CatchupGrpcClient.class.getName());

    private final ManagedChannel channel;
    private final CatchupServiceGrpc.CatchupServiceBlockingStub blockingStub;

    public CatchupGrpcClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = CatchupServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public List<Event> fetchEvents(long startAfter, long end, int maxResults, String topic) {
        LOGGER.info(String.format("Fetching events from topic %s between %d and %d (max: %d)",
                topic, startAfter, end, maxResults));

        FetchEventsRequest request = FetchEventsRequest.newBuilder()
                .setTopic(topic)
                .setStartAfter(startAfter)
                .setEnd(end)
                .setMaxResults(maxResults)
                .build();

        FetchEventsResponse response;
        try {
            response = blockingStub.fetchEvents(request);
        } catch (StatusRuntimeException e) {
            LOGGER.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            throw new RuntimeException("Failed to fetch events via gRPC", e);
        }

        List<Event> events = new ArrayList<>();
        for (com.p14n.postevent.catchup.grpc.Event grpcEvent : response.getEventsList()) {
            events.add(convertFromGrpcEvent(grpcEvent,topic));
        }

        LOGGER.info(String.format("Fetched %d events from topic %s", events.size(), topic));
        return events;
    }

    private Event convertFromGrpcEvent(com.p14n.postevent.catchup.grpc.Event grpcEvent, String topic) {
        OffsetDateTime time = null;
        if (!grpcEvent.getTime().isEmpty()) {
            time = OffsetDateTime.parse(grpcEvent.getTime());
        } else {
            time = OffsetDateTime.now();
        }

        return Event.create(
                grpcEvent.getId(),
                grpcEvent.getSource(),
                grpcEvent.getType(),
                grpcEvent.getDataContentType(),
                grpcEvent.getDataschema(),
                grpcEvent.getSubject(),
                grpcEvent.getData().toByteArray(),
                time.toInstant(),
                grpcEvent.getIdn(),
                topic);
    }

    @Override
    public void close()  {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,"Failed to close",e);
        }
    }
}