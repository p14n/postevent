package com.p14n.postevent;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostgresConnectionTest {

    private EmbeddedPostgres postgres;

    @BeforeEach
    void setUp() throws IOException {
        postgres = EmbeddedPostgres.builder()
                .setServerConfig("postgresql.version", "16.2")
                .start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void shouldConnectToPostgres() throws SQLException {
        try (Connection conn = postgres.getPostgresDatabase().getConnection();
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT 1")) {
            
            resultSet.next();
            int result = resultSet.getInt(1);
            assertEquals(1, result, "SELECT 1 should return 1");
        }
    }
}
