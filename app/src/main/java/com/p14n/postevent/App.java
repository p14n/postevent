package com.p14n.postevent;

import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.DatabaseSetup;

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
        String affinity = "";

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

    private static void writeContinuously(DataSource ds, String affinity, String[] write) {
        var gap = 10000;
        var direction = -1;
        var running = true;
        while (running) {
            try {
                Thread.sleep(gap);
                for (var wt : write) {
                    try {
                        var id = UUID.randomUUID().toString();
                        Publisher.publish(
                                Event.create(id,
                                        affinity,
                                        "test",
                                        "string",
                                        null,
                                        id,
                                        "test".getBytes()),
                                ds,
                                wt);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                gap += direction * 10;
                if (gap < 100) {
                    direction = 1;
                } else if (gap > 10000) {
                    direction = -1;
                }
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
                5443,
                "postgres",
                "postgres",
                "postgres");

        // 1 write only - server and timed publish
        // 2 read/write - server and client
        // 3 read/writw - server and client
        // 4 read only - client only
        ConsumerServer cs = null;
        ConsumerClient cc = null;
        var ds = DatabaseSetup.createPool(cfg);
        var ot = Opentelemetry.create("postevent");

        try {
            if (write.length > 0) {
                try {
                    cs = new ConsumerServer(ds, cfg, ot);
                    cs.start(50052);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (topichosts.length > 0) {
                cc = new ConsumerClient(ot);
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
                cc.start(Set.of(read), ds, topichosts[0], 50052);

                try {
                    Thread.currentThread().join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else {
                writeContinuously(ds, affinity, write);
            }

        } finally {
            close(cc);
            close(cs);
        }
    }

}
