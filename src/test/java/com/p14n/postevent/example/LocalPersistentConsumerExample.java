package com.p14n.postevent.example;

import com.p14n.postevent.LocalPersistentConsumer;
import com.p14n.postevent.Publisher;
import com.p14n.postevent.TestUtil;
import com.p14n.postevent.data.ConfigData;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class LocalPersistentConsumerExample {

    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        CountDownLatch l = new CountDownLatch(1);
        try (var pg = ExampleUtil.embeddedPostgres();
                var lc = new LocalPersistentConsumer(pg.getPostgresDatabase(), new ConfigData(
                        "local",
                        Set.of("topic"),
                        "127.0.0.1",
                        pg.getPort(),
                        "postgres",
                        "postgres",
                        "postgres"));) {

            var ds = pg.getPostgresDatabase();

            lc.start();

            lc.subscribe("topic", message -> {
                System.err.println("********* Message received *************");
                l.countDown();
            });

            Thread.sleep(1000);
            Publisher.publish(TestUtil.createTestEvent(1), ds, "topic");
            l.await();
            Thread.sleep(1000);

        }
    }

}
