package com.p14n.postevent.example;

import com.p14n.postevent.LocalConsumer;
import com.p14n.postevent.Publisher;
import com.p14n.postevent.TestUtil;
import com.p14n.postevent.broker.*;
import com.p14n.postevent.catchup.PersistentBroker;
import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.data.Event;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LocalPersistentConsumerExample {

    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        CountDownLatch l = new CountDownLatch(1);

        try (var pg = ExampleUtil.embeddedPostgres();
             var tb = new TransactionalBroker(pg.getPostgresDatabase());
             var pb = new PersistentBroker<>(tb,pg.getPostgresDatabase());
             var lc = new LocalConsumer<>(new ConfigData(
                     "local",
                     "topic",
                     "127.0.0.1",
                     pg.getPort(),
                     "postgres",
                     "postgres",
                     "postgres"
             ), pb)){

            tb.subscribe(message -> {
                System.err.println("********* Message received *************");
                l.countDown();
            });

            lc.start();
            Publisher.publish(TestUtil.createTestEvent(1), pg.getPostgresDatabase(), "topic");
            l.await();

        }
    }

}
