package com.p14n.postevent.catchup.remote;

import com.p14n.postevent.catchup.CatchupServerInterface;
import com.p14n.postevent.catchup.grpc.*;
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

/**
 * gRPC client implementation of the CatchupServerInterface.
 * Provides functionality to fetch missed events and latest message IDs from a
 * remote gRPC server.
 * Implements AutoCloseable to properly manage the gRPC channel lifecycle.
 *
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Remote event fetching via gRPC</li>
 * <li>Latest message ID retrieval</li>
 * <li>Automatic resource cleanup</li>
 * <li>Configurable connection settings</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * try (CatchupGrpcClient client = new CatchupGrpcClient("localhost", 50051)) {
 *     List<Event> events = client.fetchEvents(100L, 200L, 50, "my-topic");
 *     long latestId = client.getLatestMessageId("my-topic");
 * }
 * }</pre>
 */
public class CatchupGrpcClient implements CatchupServerInterface, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CatchupGrpcClient.class);

    private final ManagedChannel channel;
    private final CatchupServiceGrpc.CatchupServiceBlockingStub blockingStub;

    /**
     * Creates a new CatchupGrpcClient with the specified host and port.
     * Initializes a plain text connection (no TLS) to the gRPC server.
     *
     * @param host The hostname of the gRPC server
     * @param port The port number of the gRPC server
     */
    public CatchupGrpcClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build());
    }

    /**
     * Creates a new CatchupGrpcClient with a pre-configured channel.
     * Useful for custom channel configuration or testing.
     *
     * @param channel The pre-configured gRPC managed channel
     */
    public CatchupGrpcClient(ManagedChannel channel) {
        this.channel = channel;
        this.blockingStub = CatchupServiceGrpc.newBlockingStub(channel);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * This implementation makes a blocking gRPC call to fetch events from the
     * remote server.
     * Events are converted from the gRPC protocol format to the internal Event
     * format.
     * </p>
     *
     * @throws IllegalArgumentException if startAfter is greater than end,
     *                                  maxResults is less than or equal to 0,
     *                                  or if topic is null or empty
     * @throws RuntimeException         if the gRPC call fails or if there's an
     *                                  error converting the response
     */
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

    /**
     * Converts a gRPC event message to the internal Event format.
     * If the time field is empty in the gRPC event, the current time is used.
     *
     * @param grpcEvent The gRPC event message to convert
     * @param topic     The topic associated with the event
     * @return A new Event instance with the converted data
     */
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

    /**
     * {@inheritDoc}
     *
     * <p>
     * This implementation makes a blocking gRPC call to fetch the latest message ID
     * from the remote server.
     * </p>
     *
     * @throws IllegalArgumentException if topic is null or empty
     * @throws RuntimeException         if the gRPC call fails
     */
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

    /**
     * Closes the gRPC channel with a 5-second timeout.
     * This method should be called when the client is no longer needed to free up
     * resources.
     */
    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Failed to close", e);
        }
    }
}
