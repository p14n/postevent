package com.p14n.postevent.example;

import com.p14n.postevent.LocalConsumer;
import com.p14n.postevent.Publisher;
import com.p14n.postevent.TestUtil;
import com.p14n.postevent.broker.DefaultMessageBroker;
import com.p14n.postevent.broker.EventMessageBroker;
import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.data.Event;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LocalConsumerExample {

    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        CountDownLatch l = new CountDownLatch(1);

        try (var mb = new EventMessageBroker();
             EmbeddedPostgres pg = ExampleUtil.embeddedPostgres();
             var lc = new LocalConsumer<>(new ConfigData(
                        "local",
                        "topic",
                        "127.0.0.1",
                        pg.getPort(),
                        "postgres",
                        "postgres",
                        "postgres"), mb)) {

            mb.subscribe(message -> {
                System.err.println("********* Message received *************");
                l.countDown();
            });

            lc.start();
            Publisher.publish(TestUtil.createTestEvent(1), pg.getPostgresDatabase(), "topic");
            l.await();

        }
    }
}