package com.p14n.postevent;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.p14n.postevent.broker.AsyncExecutor;
import com.p14n.postevent.broker.DefaultExecutor;
import com.p14n.postevent.broker.EventMessageBroker;
import com.p14n.postevent.broker.remote.MessageBrokerGrpcServer;
import com.p14n.postevent.catchup.CatchupServer;
import com.p14n.postevent.catchup.remote.CatchupGrpcServer;
import com.p14n.postevent.data.ConfigData;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opentelemetry.api.OpenTelemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A server that manages event consumption and distribution.
 * ConsumerServer handles incoming events, manages their persistence,
 * and coordinates event distribution to subscribers.
 * 
 * <p>
 * This server can be configured to handle multiple topics and supports
 * both local and remote consumers through gRPC connections.
 * </p>
 * 
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Event persistence using a configured DataSource</li>
 * <li>Asynchronous event processing via AsyncExecutor</li>
 * <li>gRPC-based remote event distribution</li>
 * <li>OpenTelemetry integration for monitoring and tracing</li>
 * <li>Catchup functionality for missed events</li>
 * </ul>
 * 
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * var config = new ConfigData("myapp", Set.of("orders"), "localhost", 5432,
 *         "postgres", "postgres", "postgres", 10);
 * var server = new ConsumerServer(dataSource, config, OpenTelemetry.noop());
 * server.start(8080);
 * }</pre>
 */
public class ConsumerServer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerServer.class);

    private DataSource ds;
    private ConfigData cfg;
    private List<AutoCloseable> closeables;
    private Server server;
    private AsyncExecutor asyncExecutor;
    OpenTelemetry ot;

    /**
     * Creates a new ConsumerServer instance with default executor configuration.
     *
     * @param ds  The datasource for event persistence
     * @param cfg The configuration for the consumer server
     * @param ot  The OpenTelemetry instance for monitoring and tracing
     */
    public ConsumerServer(DataSource ds, ConfigData cfg, OpenTelemetry ot) {
        this(ds, cfg, new DefaultExecutor(2), ot);
    }

    /**
     * Creates a new ConsumerServer instance with custom executor configuration.
     *
     * @param ds            The datasource for event persistence
     * @param cfg           The configuration for the consumer server
     * @param asyncExecutor The executor for handling asynchronous operations
     * @param ot            The OpenTelemetry instance for monitoring and tracing
     */
    public ConsumerServer(DataSource ds, ConfigData cfg, AsyncExecutor asyncExecutor, OpenTelemetry ot) {
        this.ds = ds;
        this.cfg = cfg;
        this.asyncExecutor = asyncExecutor;
        this.ot = ot;
    }

    /**
     * Starts the consumer server on the specified port.
     * Initializes the event broker, local consumer, and gRPC services.
     *
     * @param port The port number to listen on
     * @throws IOException          If the server fails to start
     * @throws InterruptedException If the server startup is interrupted
     */
    public void start(int port) throws IOException, InterruptedException {
        start(ServerBuilder.forPort(port));
    }

    /**
     * Starts the consumer server with a custom server builder configuration.
     * Initializes the event broker, local consumer, and gRPC services.
     *
     * @param sb The server builder to use for configuration
     * @throws IOException          If the server fails to start
     * @throws InterruptedException If the server startup is interrupted
     */
    public void start(ServerBuilder<?> sb) throws IOException, InterruptedException {
        logger.atInfo().log("Starting consumer server");

        var mb = new EventMessageBroker(asyncExecutor, ot, "consumer_server");
        var lc = new LocalConsumer<>(cfg, mb);
        var grpcServer = new MessageBrokerGrpcServer(mb);
        var catchupServer = new CatchupServer(ds);
        var catchupService = new CatchupGrpcServer.CatchupServiceImpl(catchupServer);

        try {
            lc.start();
            server = sb.addService(grpcServer)
                    .addService(catchupService)
                    .permitKeepAliveTime(1, TimeUnit.HOURS)
                    .permitKeepAliveWithoutCalls(true)
                    .build()
                    .start();

            logger.atInfo().log("Consumer server started successfully");
        } catch (Exception e) {
            logger.atError()
                    .setCause(e)
                    .log("Failed to start consumer server");
            throw e;
        }

        closeables = List.of(lc, mb, asyncExecutor);
    }

    /**
     * Stops the consumer server and releases all resources.
     * Shuts down the gRPC server and closes all managed resources.
     */
    public void stop() {
        logger.atInfo().log("Stopping consumer server");

        server.shutdown();
        for (var c : closeables) {
            try {
                c.close();
            } catch (Exception e) {
                logger.atWarn()
                        .setCause(e)
                        .addArgument(c.getClass().getSimpleName())
                        .log("Error closing {}");
            }
        }

        logger.atInfo().log("Consumer server stopped");
    }

    /**
     * Stops the consumer server and releases all resources.
     * Implementation of AutoCloseable interface.
     *
     * @throws Exception If an error occurs during shutdown
     */
    @Override
    public void close() throws Exception {
        stop();
    }

}
