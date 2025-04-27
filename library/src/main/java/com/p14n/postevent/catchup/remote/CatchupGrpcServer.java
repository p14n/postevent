package com.p14n.postevent.catchup.remote;

import com.p14n.postevent.catchup.CatchupServerInterface;
import com.p14n.postevent.catchup.grpc.*;
import com.p14n.postevent.data.Event;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * gRPC server implementation for the catchup service.
 * Provides a gRPC interface for clients to fetch missed events and get latest
 * message IDs.
 * 
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Event catchup functionality via gRPC</li>
 * <li>Latest message ID retrieval</li>
 * <li>Graceful shutdown handling</li>
 * <li>Automatic resource cleanup</li>
 * </ul>
 * 
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * CatchupServerInterface catchupServer = new CatchupServer(dataSource);
 * CatchupGrpcServer server = new CatchupGrpcServer(8080, catchupServer);
 * server.start();
 * // ... server is running ...
 * server.stop();
 * }</pre>
 */
public class CatchupGrpcServer {
    private static final Logger logger = LoggerFactory.getLogger(CatchupGrpcServer.class);

    private final int port;
    private final Server server;

    /**
     * Creates a new CatchupGrpcServer instance.
     *
     * @param port          The port number to listen on
     * @param catchupServer The backend server implementation for handling catchup
     *                      operations
     */
    public CatchupGrpcServer(int port, CatchupServerInterface catchupServer) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(new CatchupServiceImpl(catchupServer))
                .build();
    }

    /**
     * Starts the gRPC server and registers a shutdown hook.
     * The server will listen for incoming connections on the configured port.
     *
     * @throws IOException If the server fails to start
     */
    public void start() throws IOException {
        server.start();
        logger.atInfo().log("Server started, listening on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.atInfo().log("Shutting down gRPC server since JVM is shutting down");
            try {
                CatchupGrpcServer.this.stop();
            } catch (InterruptedException e) {
                logger.atError().setCause(e).log("Error shutting down server");
            }
            logger.atInfo().log("Server shut down");
        }));
    }

    /**
     * Initiates an orderly shutdown of the server.
     * Waits for ongoing requests to complete up to a timeout of 30 seconds.
     *
     * @throws InterruptedException If the shutdown process is interrupted
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Blocks until the server is shutdown.
     * Useful for keeping the server running in the main thread.
     *
     * @throws InterruptedException If the blocking is interrupted
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Implementation of the gRPC catchup service.
     * Handles incoming gRPC requests by delegating to the backend
     * CatchupServerInterface.
     */
    public static class CatchupServiceImpl extends CatchupServiceGrpc.CatchupServiceImplBase {
        private final CatchupServerInterface catchupServer;

        /**
         * Creates a new CatchupServiceImpl instance.
         *
         * @param catchupServer The backend server interface for catchup operations
         */
        public CatchupServiceImpl(CatchupServerInterface catchupServer) {
            this.catchupServer = catchupServer;
        }

        /**
         * Handles gRPC requests for getting the latest message ID for a topic.
         *
         * @param request          The request containing the topic name
         * @param responseObserver The observer for sending the response
         */
        @Override
        public void getLatestMessageId(TopicRequest request, StreamObserver<LatestMessageIdResponse> responseObserver) {
            try {
                long latestId = catchupServer.getLatestMessageId(request.getTopic());

                LatestMessageIdResponse response = LatestMessageIdResponse.newBuilder()
                        .setMessageId(latestId)
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Error getting latest message ID", e);
                responseObserver.onError(e);
            }
        }

        /**
         * Handles gRPC requests for fetching events within a specified range.
         *
         * @param request          The request containing range parameters and topic
         * @param responseObserver The observer for sending the response
         */
        @Override
        public void fetchEvents(FetchEventsRequest request, StreamObserver<FetchEventsResponse> responseObserver) {
            try {
                List<Event> events = catchupServer.fetchEvents(
                        request.getStartAfter(),
                        request.getEnd(),
                        request.getMaxResults(),
                        request.getTopic());

                List<com.p14n.postevent.catchup.grpc.Event> grpcEvents = events.stream()
                        .map(this::convertToGrpcEvent)
                        .collect(Collectors.toList());

                FetchEventsResponse response = FetchEventsResponse.newBuilder()
                        .addAllEvents(grpcEvents)
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Error fetching events", e);
                responseObserver.onError(e);
            }
        }

        /**
         * Converts a domain Event object to its gRPC representation.
         *
         * @param event The domain event to convert
         * @return The gRPC event representation
         */
        private com.p14n.postevent.catchup.grpc.Event convertToGrpcEvent(Event event) {
            return com.p14n.postevent.catchup.grpc.Event.newBuilder()
                    .setId(event.id())
                    .setSource(event.source())
                    .setType(event.type())
                    .setDataContentType(event.datacontenttype())
                    .setTime(event.time() != null ? event.time().toString() : "")
                    .setSubject(event.subject() != null ? event.subject() : "")
                    .setData(com.google.protobuf.ByteString.copyFrom(event.data()))
                    .setIdn(event.idn())
                    .build();
        }
    }
}
