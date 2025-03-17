package com.p14n.postevent.catchup.grpc;

import com.p14n.postevent.catchup.CatchupServerInterface;
import com.p14n.postevent.data.Event;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class CatchupGrpcServer {
    private static final Logger LOGGER = Logger.getLogger(CatchupGrpcServer.class.getName());

    private final int port;
    private final Server server;

    public CatchupGrpcServer(int port, CatchupServerInterface catchupServer) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(new CatchupServiceImpl(catchupServer))
                .build();
    }

    public void start() throws IOException {
        server.start();
        LOGGER.info("Server started, listening on port " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("*** Shutting down gRPC server since JVM is shutting down");
            try {
                CatchupGrpcServer.this.stop();
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, "Error shutting down server", e);
            }
            LOGGER.info("*** Server shut down");
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static class CatchupServiceImpl extends CatchupServiceGrpc.CatchupServiceImplBase {
        private final CatchupServerInterface catchupServer;

        CatchupServiceImpl(CatchupServerInterface catchupServer) {
            this.catchupServer = catchupServer;
        }

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
                LOGGER.log(Level.SEVERE, "Error fetching events", e);
                responseObserver.onError(e);
            }
        }

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