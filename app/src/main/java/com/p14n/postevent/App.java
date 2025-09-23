package com.p14n.postevent;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import org.postgresql.Driver;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import javax.net.ssl.SSLException;
import javax.sql.DataSource;

import com.p14n.postevent.data.ConfigData;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.DatabaseSetup;
import com.p14n.postevent.db.PoolSetup;
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

    public static void main(String[] args) throws Exception {
        System.out.println("Hello World!");
        DriverManager.registerDriver(new Driver());
        String affinity = UUID.randomUUID().toString().substring(0, 8);

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

    private static void writeContinuously(DataSource ds, String affinity, String[] write, OpenTelemetry ot)
            throws InterruptedException {
        var gap = 1000;
        var direction = -1;
        var running = true;
        Tracer tracer = ot.getTracer("postevent");
        while (running) {
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
        }
    }

    private static void run(String affinity, String[] write, String[] read, String dbhost, String[] topichosts)
            throws IOException, InterruptedException {

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
        RemotePersistentConsumer cc = null;

        var ot = Opentelemetry.create("postevent");
        var ds = JdbcTelemetry.create(ot).wrap(PoolSetup.createPool(cfg));

        try {
            if (write.length > 0) {
                cs = new ConsumerServer(ds, cfg, ot);
                cs.start(50052);
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

    private static SslContext buildSslContext() {
        try {
            return GrpcSslContexts.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE) // Skip verification - only for internal/dev use
                    .build();
        } catch (SSLException e) {
            throw new RuntimeException("Failed to create SSL context", e);
        }
    }

    private static NettyChannelBuilder buildClientChannel(String host, int port) {
        return NettyChannelBuilder.forAddress(host, port)
                .keepAliveTime(1, TimeUnit.HOURS)
                .keepAliveTimeout(30, TimeUnit.SECONDS)
                .useTransportSecurity() // Change from usePlaintext()
                .overrideAuthority("postevent.internal") // Match certificate common name
                .sslContext(buildSslContext());
    }

    private static RemotePersistentConsumer runConsumerClient(String[] write, String[] read, String[] topichosts,
            DataSource ds,
            OpenTelemetry ot) {

        RemotePersistentConsumer cc;
        cc = new RemotePersistentConsumer(ot, 10);
        cc.start(Set.of(read), ds, buildClientChannel(topichosts[0], 50052).build());

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
