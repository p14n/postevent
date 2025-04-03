package com.p14n.postevent.example;

import com.p14n.postevent.ConsumerClient;
import com.p14n.postevent.ConsumerServer;
import com.p14n.postevent.Publisher;
import com.p14n.postevent.TestUtil;
import com.p14n.postevent.data.ConfigData;

import javax.sql.DataSource;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

public class RemoteConsumerExample {

    private static void constructServer(DataSource ds, ConfigData cfg, CountDownLatch l, int port) {

        try (ConsumerServer cs = new ConsumerServer(ds, cfg)) {
            cs.start(port);
            l.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void constructClient(DataSource ds, CountDownLatch l, int port, String topic) {

        try (ConsumerClient client = new ConsumerClient()) {
            client.start(Set.of(topic), ds, "localhost", port);
            client.subscribe(topic, message -> {
                System.err.println("********* Message received *************");
                l.countDown();
            });
            l.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {

        int port = 50052;

        try (var pg = ExampleUtil.embeddedPostgres();
                var es = Executors.newVirtualThreadPerTaskExecutor()) {

            var ds = pg.getPostgresDatabase();
            var cfg = new ConfigData(
                    "local",
                    Set.of("topic"),
                    "127.0.0.1",
                    pg.getPort(),
                    "postgres",
                    "postgres",
                    "postgres");
            CountDownLatch serverLatch = new CountDownLatch(1);

            es.execute(() -> constructServer(ds, cfg, serverLatch, port));

            Thread.sleep(2000);

            es.execute(() -> constructClient(ds, serverLatch, port, cfg.topics().iterator().next()));

            Thread.sleep(2000);

            Publisher.publish(TestUtil.createTestEvent(1), ds, cfg.topics().iterator().next());

            serverLatch.await();

        }
    }

}
