package com.p14n.postevent;

import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.DatabaseSetup;

import com.p14n.postevent.telemetry.OpenTelemetryFunctions;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.instrumentation.jdbc.datasource.JdbcTelemetry;

public class App {

    private static String[] envVals(String name) {
        var e = System.getenv(name);
        if (e != null) {
            return e.split(",");
        }
        return new String[] {};
    }

    public static void main(String[] args) {
        System.out.println("Hello World!");
        String affinity = "local";

        var write = envVals("APP_WRITE_TOPICS");
        var read = envVals("APP_READ_TOPICS");
        var dbhost = envVals("APP_DB_HOST")[0];
        var topichosts = envVals("APP_TOPIC_HOST");

        run(affinity, write, read, dbhost, topichosts);
    }

    private static void close(AutoCloseable c) {
        try {
            if (c != null)
                c.close();
        } catch (Exception e) {
        }
    }

    private static void writeContinuously(DataSource ds, String affinity, String[] write, OpenTelemetry ot) {
        var gap = 1000;
        var direction = -1;
        var running = true;
        Tracer tracer = ot.getTracer("postevent");
        while (running) {
            try {
                for (var wt : write) {
                    IntStream.range(0, 10).forEachOrdered(n -> {
                        var id = UUID.randomUUID().toString();
                        OpenTelemetryFunctions.processWithTelemetry(tracer, "publish_new_event", () -> {
                            try {
                                Publisher.publish(
                                        Event.create(id,
                                                affinity,
                                                "test",
                                                "string",
                                                null,
                                                id,
                                                "test".getBytes(),
                                                OpenTelemetryFunctions.serializeTraceContext(ot)),
                                        ds,
                                        wt);
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                            return null;
                        });
                    });
                }
                gap += direction * 10;
                if (gap < 10) {
                    direction = 1;
                } else if (gap > 1000) {
                    direction = -1;
                }
                Thread.sleep(gap);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void run(String affinity, String[] write, String[] read, String dbhost, String[] topichosts) {

        var cfg = new ConfigData(
                affinity,
                Set.of(write),
                dbhost,
                5432,
                "postgres",
                "postgres",
                "postgres",
                10);

        // 1 write only - server and timed publish
        // 2 read/write - server and client
        // 3 read/writw - server and client
        // 4 read only - client only
        ConsumerServer cs = null;
        ConsumerClient cc = null;

        var ot = Opentelemetry.create("postevent");
        var ds = JdbcTelemetry.create(ot).wrap(DatabaseSetup.createPool(cfg));

        try {
            if (write.length > 0) {
                try {
                    cs = new ConsumerServer(ds, cfg, ot);
                    cs.start(50052);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (topichosts.length == 1 && topichosts[0].equals("localhost")) {
                cc = runConsumerClient(new String[] {}, read, topichosts, ds, ot);
                writeContinuously(ds, affinity, write, ot);
            } else if (topichosts.length > 0) {
                cc = runConsumerClient(write, read, topichosts, ds, ot);
                try {
                    Thread.currentThread().join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else {
                writeContinuously(ds, affinity, write, ot);
            }

        } finally {
            close(cc);
            close(cs);
        }
    }

    private static ConsumerClient runConsumerClient(String[] write, String[] read, String[] topichosts, DataSource ds,
            OpenTelemetry ot) {
        ConsumerClient cc;
        cc = new ConsumerClient(ot);
        cc.start(Set.of(read), ds, topichosts[0], 50052);
        for (var topic : read) {
            cc.subscribe(topic, (ev) -> {
                for (var wt : write) {
                    try {
                        Publisher.publish(ev.event(), ev.connection(), wt);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        return cc;
    }

}
