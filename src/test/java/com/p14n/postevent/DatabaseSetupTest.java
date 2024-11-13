package com.p14n.postevent;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseSetupTest {
    private EmbeddedPostgres postgres;
    private DatabaseSetup databaseSetup;
    private Connection connection;

    @BeforeEach
    void setUp() throws IOException, SQLException {
        postgres = EmbeddedPostgres.start();
        String jdbcUrl = postgres.getJdbcUrl("postgres", "postgres");
        databaseSetup = new DatabaseSetup(jdbcUrl, "postgres", "postgres");
        connection = postgres.getPostgresDatabase().getConnection();
    }

    @AfterEach
    void tearDown() throws IOException, SQLException {
        if (connection != null) {
            connection.close();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void shouldCreateSchemaIfNotExists() throws SQLException {
        databaseSetup.createSchemaIfNotExists();

        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'postevent'"
            );
            assertTrue(rs.next(), "Schema 'postevent' should exist");
            assertEquals("postevent", rs.getString("schema_name"));
        }
    }

    @Test
    void shouldCreateTableIfNotExists() throws SQLException {
        String topic = "test_topic";
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createTableIfNotExists(topic);

        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                "SELECT table_name FROM information_schema.tables " +
                "WHERE table_schema = 'postevent' AND table_name = '" + topic + "'"
            );
            assertTrue(rs.next(), "Table '" + topic + "' should exist");
            assertEquals(topic, rs.getString("table_name"));
        }
    }

    @Test
    void shouldVerifyTableColumns() throws SQLException {
        String topic = "test_topic";
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createTableIfNotExists(topic);

        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                "SELECT column_name, data_type, is_nullable, column_default " +
                "FROM information_schema.columns " +
                "WHERE table_schema = 'postevent' AND table_name = '" + topic + "' " +
                "ORDER BY ordinal_position"
            );

            assertTrue(rs.next());
            assertEquals("idn", rs.getString("column_name"));
            assertEquals("bigint", rs.getString("data_type"));
            assertEquals("NO", rs.getString("is_nullable"));

            assertTrue(rs.next());
            assertEquals("id", rs.getString("column_name"));
            assertEquals("character varying", rs.getString("data_type"));
            assertEquals("NO", rs.getString("is_nullable"));
        }
    }

    @Test
    void shouldBeIdempotent() {
        String topic = "test_topic";
        
        // Execute twice
        assertDoesNotThrow(() -> {
            databaseSetup.createSchemaIfNotExists();
            databaseSetup.createTableIfNotExists(topic);
            databaseSetup.createSchemaIfNotExists();
            databaseSetup.createTableIfNotExists(topic);
        });
    }

    @Test
    void shouldThrowExceptionForInvalidTopicName() {
        assertThrows(IllegalArgumentException.class, () -> 
            databaseSetup.createTableIfNotExists(null)
        );
        
        assertThrows(IllegalArgumentException.class, () -> 
            databaseSetup.createTableIfNotExists("")
        );
        
        assertThrows(IllegalArgumentException.class, () -> 
            databaseSetup.createTableIfNotExists("   ")
        );
    }
}
