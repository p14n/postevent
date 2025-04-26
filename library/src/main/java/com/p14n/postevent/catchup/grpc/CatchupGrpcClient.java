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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatchupGrpcClient implements CatchupServerInterface, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CatchupGrpcClient.class);

    private final ManagedChannel channel;
    private final CatchupServiceGrpc.CatchupServiceBlockingStub blockingStub;

    public CatchupGrpcClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build());
    }

    public CatchupGrpcClient(ManagedChannel channel) {
        this.channel = channel;
        this.blockingStub = CatchupServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public List<Event> fetchEvents(long startAfter, long end, int maxResults, String topic) {
        logger.atInfo()
                .addArgument(topic)
                .addArgument(startAfter)
                .addArgument(end)
                .addArgument(maxResults)
                .log("Fetching events from topic {} between {} and {} (max: {})");

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
            logger.atWarn().setCause(e).log("RPC failed: {}", e.getStatus());
            throw new RuntimeException("Failed to fetch events via gRPC", e);
        }

        List<Event> events = new ArrayList<>();
        for (com.p14n.postevent.catchup.grpc.Event grpcEvent : response.getEventsList()) {
            events.add(convertFromGrpcEvent(grpcEvent, topic));
        }

        logger.info("Fetched {} events from topic {}", events.size(), topic);
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
                topic,
                grpcEvent.getTraceparent());
    }

    @Override
    public long getLatestMessageId(String topic) {
        logger.atInfo()
                .addArgument(topic)
                .log("Fetching latest message ID for topic {}");

        TopicRequest request = TopicRequest.newBuilder()
                .setTopic(topic)
                .build();

        LatestMessageIdResponse response;
        try {
            response = blockingStub.getLatestMessageId(request);
            return response.getMessageId();
        } catch (StatusRuntimeException e) {
            logger.atWarn().setCause(e).log("RPC failed: {}", e.getStatus());
            throw new RuntimeException("Failed to fetch latest message ID via gRPC", e);
        }
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Failed to close", e);
        }
    }
}
