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

public class ConsumerServer implements AutoCloseable {

    private DataSource ds;
    private ConfigData cfg;
    private List<AutoCloseable> closeables;
    private Server server;
    private AsyncExecutor asyncExecutor;

    public ConsumerServer(DataSource ds, ConfigData cfg) {
        this(ds, cfg, new DefaultExecutor(2));
    }

    public ConsumerServer(DataSource ds, ConfigData cfg, AsyncExecutor asyncExecutor) {
        this.ds = ds;
        this.cfg = cfg;
        this.asyncExecutor = asyncExecutor;
    }

    public void start(int port) throws IOException, InterruptedException {
        start(ServerBuilder.forPort(port));
    }

    public void start(ServerBuilder<?> sb) throws IOException, InterruptedException {
        var mb = new EventMessageBroker();
        var lc = new LocalConsumer<>(cfg, mb);
        var grpcServer = new MessageBrokerGrpcServer(mb);
        var catchupServer = new CatchupServer(ds);
        var catchupService = new CatchupGrpcServer.CatchupServiceImpl(catchupServer);
        lc.start();
        server = sb.addService(grpcServer)
                .addService(catchupService)
                .permitKeepAliveTime(1, TimeUnit.HOURS)
                .permitKeepAliveWithoutCalls(true)
                .build()
                .start();
        closeables = List.of(lc, mb, asyncExecutor);
    }

    public void stop() {
        server.shutdown();
        for (var c : closeables) {
            try {
                c.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }

}
