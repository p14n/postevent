package com.p14n.postevent;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.p14n.postevent.broker.AsyncExecutor;
import com.p14n.postevent.broker.DefaultExecutor;
import com.p14n.postevent.broker.EventMessageBroker;
import com.p14n.postevent.broker.grpc.MessageBrokerGrpcServer;
import com.p14n.postevent.catchup.CatchupServer;
import com.p14n.postevent.catchup.grpc.CatchupGrpcServer;
import com.p14n.postevent.data.ConfigData;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opentelemetry.api.OpenTelemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerServer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerServer.class);

    private DataSource ds;
    private ConfigData cfg;
    private List<AutoCloseable> closeables;
    private Server server;
    private AsyncExecutor asyncExecutor;
    OpenTelemetry ot;

    public ConsumerServer(DataSource ds, ConfigData cfg, OpenTelemetry ot) {
        this(ds, cfg, new DefaultExecutor(2), ot);
    }

    public ConsumerServer(DataSource ds, ConfigData cfg, AsyncExecutor asyncExecutor, OpenTelemetry ot) {
        this.ds = ds;
        this.cfg = cfg;
        this.asyncExecutor = asyncExecutor;
        this.ot = ot;
    }

    public void start(int port) throws IOException, InterruptedException {
        start(ServerBuilder.forPort(port));
    }

    public void start(ServerBuilder<?> sb) throws IOException, InterruptedException {
        logger.atInfo().log("Starting consumer server");

        var mb = new EventMessageBroker(asyncExecutor, ot);
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

    @Override
    public void close() throws Exception {
        stop();
    }

}
