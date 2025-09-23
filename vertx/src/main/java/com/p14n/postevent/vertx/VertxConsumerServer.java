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

public class VertxConsumerServer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(VertxConsumerServer.class);

    private DataSource ds;
    //private ConfigData cfg;
    private List<AutoCloseable> closeables;
    private AsyncExecutor asyncExecutor;
    OpenTelemetry ot;

    public VertxConsumerServer(DataSource ds, AsyncExecutor asyncExecutor, OpenTelemetry ot) {
        this.ds = ds;
        this.asyncExecutor = asyncExecutor;
        this.ot = ot;
    }

    public void start(EventBus eb, EventBusMessageBroker mb, Set<String> topics) throws IOException, InterruptedException {
        logger.atInfo().log("Starting consumer server");

        var db = new DatabaseSetup(ds);
        db.setupServer(topics);
        var catchupServer = new CatchupServer(ds);
        var catchupService = new EventBusCatchupService(catchupServer,eb,topics,this.asyncExecutor);

        closeables = List.of(catchupService, mb, asyncExecutor);
        System.out.println("🌐 Vert.x EventBus server started");

    }

    @Override
    public void close() {
        if(closeables != null){
            for(var c : closeables){
                try {
                    c.close();
                } catch (Exception e){

                }
            }
        }
        System.out.println("🛑 Vert.x EventBus server stopped");
    }
}
