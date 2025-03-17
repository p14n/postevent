package com.p14n.postevent.example;

import com.p14n.postevent.LocalConsumer;
import com.p14n.postevent.Publisher;
import com.p14n.postevent.TestUtil;
import com.p14n.postevent.broker.EventMessageBroker;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.broker.TransactionalBroker;
import com.p14n.postevent.broker.grpc.MessageBrokerGrpcClient;
import com.p14n.postevent.broker.grpc.MessageBrokerGrpcServer;
import com.p14n.postevent.catchup.CatchupServer;
import com.p14n.postevent.catchup.CatchupService;
import com.p14n.postevent.catchup.PersistentBroker;
import com.p14n.postevent.catchup.grpc.CatchupGrpcClient;
import com.p14n.postevent.catchup.grpc.CatchupGrpcServer;
import com.p14n.postevent.data.ConfigData;
import io.grpc.ServerBuilder;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RemoteConsumerExample {

    private static void constructServer(DataSource ds,ConfigData cfg,CountDownLatch l, int port) {

        try (var mb = new EventMessageBroker();
             var lc = new LocalConsumer<>(cfg, mb)){

            var grpcServer = new MessageBrokerGrpcServer(mb);
            var catchupServer = new CatchupServer(ds);
            var catchupService = new CatchupGrpcServer.CatchupServiceImpl(catchupServer);
            var server = ServerBuilder.forPort(port)
                    .addService(grpcServer)
                    .addService(catchupService)
                    .permitKeepAliveTime(1, TimeUnit.HOURS)
                    .permitKeepAliveWithoutCalls(true)
                    .build()
                    .start();

            lc.start();
            l.await();
            server.shutdownNow();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void constructClient(DataSource ds,CountDownLatch l, int port, String topic)  {

        try (var tb = new TransactionalBroker(ds);
             var seb = new SystemEventBroker();
             var pb = new PersistentBroker<>(tb,ds,seb);
             var client = new MessageBrokerGrpcClient("localhost", port, topic);
             var catchupClient = new CatchupGrpcClient("localhost", port)){

            client.subscribe(pb);

            seb.subscribe(new CatchupService(ds,catchupClient));

            tb.subscribe(message -> {
                System.err.println("********* Message received *************");
                l.countDown();
            });

            l.await();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {


        int port = 50052;

        try (var pg = ExampleUtil.embeddedPostgres();
             var es = Executors.newVirtualThreadPerTaskExecutor()){

            var ds = pg.getPostgresDatabase();
            var cfg = new ConfigData(
                    "local",
                    "topic",
                    "127.0.0.1",
                    pg.getPort(),
                    "postgres",
                    "postgres",
                    "postgres"
            );
            CountDownLatch serverLatch = new CountDownLatch(1);

            es.execute(() -> constructServer(ds, cfg, serverLatch, port));

            es.execute( () -> constructClient(ds,serverLatch,port,cfg.name()));

            Thread.sleep(2000);

            Publisher.publish(TestUtil.createTestEvent(1), ds, cfg.name());
            
            serverLatch.await();

        }
    }


}
