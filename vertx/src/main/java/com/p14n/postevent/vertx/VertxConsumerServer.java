package com.p14n.postevent.vertx;

import com.p14n.postevent.vertx.adapter.EventBusCatchupService;
import com.p14n.postevent.vertx.adapter.EventBusMessageBroker;
import com.p14n.postevent.broker.AsyncExecutor;
import com.p14n.postevent.catchup.CatchupServer;
import com.p14n.postevent.db.DatabaseSetup;
import io.opentelemetry.api.OpenTelemetry;
import io.vertx.core.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Vert.x-based consumer server that provides event consumption capabilities
 * using the EventBus for communication and coordination.
 *
 * <p>
 * This server sets up the necessary infrastructure for event processing
 * including database setup, message brokers, and catchup services.
 * </p>
 */
public class VertxConsumerServer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(VertxConsumerServer.class);

    private final DataSource ds;
    private List<AutoCloseable> closeables;
    private final AsyncExecutor asyncExecutor;
    OpenTelemetry ot;

    /**
     * Creates a new VertxConsumerServer.
     *
     * @param ds            The DataSource for database operations
     * @param asyncExecutor The async executor for handling operations
     * @param ot            The OpenTelemetry instance for observability
     */
    public VertxConsumerServer(DataSource ds, AsyncExecutor asyncExecutor, OpenTelemetry ot) {
        this.ds = ds;
        this.asyncExecutor = asyncExecutor;
        this.ot = ot;
    }

    /**
     * Starts the consumer server with the specified configuration.
     *
     * @param eb     The Vert.x EventBus to use for communication
     * @param mb     The EventBus message broker for event handling
     * @param topics The set of topics to handle
     * @throws IOException          If database setup fails
     * @throws InterruptedException If the operation is interrupted
     */
    public void start(EventBus eb, EventBusMessageBroker mb, Set<String> topics)
            throws IOException, InterruptedException {
        logger.atInfo().log("Starting consumer server");

        var db = new DatabaseSetup(ds);
        db.setupServer(topics);
        var catchupServer = new CatchupServer(ds);
        var catchupService = new EventBusCatchupService(catchupServer, eb, topics, this.asyncExecutor);

        closeables = List.of(catchupService, mb, asyncExecutor);
        System.out.println("🌐 Vert.x EventBus server started");

    }

    @Override
    public void close() {
        if (closeables != null) {
            for (var c : closeables) {
                try {
                    c.close();
                } catch (Exception e) {

                }
            }
        }
        System.out.println("🛑 Vert.x EventBus server stopped");
    }
}
