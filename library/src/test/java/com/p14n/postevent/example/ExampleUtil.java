package com.p14n.postevent.example;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import java.io.IOException;

public class ExampleUtil {

    public static EmbeddedPostgres embeddedPostgres() throws IOException {
        return EmbeddedPostgres.builder()
                .setServerConfig("wal_level", "logical")
                .setServerConfig("max_wal_senders", "3")
                .start();
    }

}
