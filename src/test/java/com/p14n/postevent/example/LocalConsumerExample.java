package com.p14n.postevent.example;

import com.p14n.postevent.LocalConsumer;
import com.p14n.postevent.Publisher;
import com.p14n.postevent.TestUtil;
import com.p14n.postevent.broker.EventMessageBroker;
import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.telemetry.DefaultTelemetryConfig;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class LocalConsumerExample {
    private static final Logger logger = LoggerFactory.getLogger(LocalConsumerExample.class);

    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        CountDownLatch l = new CountDownLatch(1);
        var telemetryConfig = new DefaultTelemetryConfig("example");

        try (var mb = new EventMessageBroker(telemetryConfig);
                EmbeddedPostgres pg = ExampleUtil.embeddedPostgres();
                var lc = new LocalConsumer<>(new ConfigData(
                        "local",
                        Set.of("topic"),
                        "127.0.0.1",
                        pg.getPort(),
                        "postgres",
                        "postgres",
                        "postgres"), mb)) {

            mb.subscribe("topic", message -> {
                logger.atInfo().log("********* Message received *************");
                l.countDown();
            });

            lc.start();
            Publisher.publish(TestUtil.createTestEvent(1), pg.getPostgresDatabase(), "topic");
            l.await();
        }
    }
}
