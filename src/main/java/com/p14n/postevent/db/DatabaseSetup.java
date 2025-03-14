package com.p14n.postevent.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseSetup {
    private static final Logger LOGGER = Logger.getLogger(DatabaseSetup.class.getName());
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public DatabaseSetup(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public DatabaseSetup createSchemaIfNotExists() {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            String sql = "CREATE SCHEMA IF NOT EXISTS postevent";
            stmt.execute(sql);
            LOGGER.info("Schema creation completed successfully");

        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error creating schema", e);
            throw new RuntimeException("Failed to create schema", e);
        }
        return this;
    }

    public DatabaseSetup createTableIfNotExists(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (!topic.matches("^[A-Za-z_][A-Za-z0-9_]*$")) {
            throw new IllegalArgumentException("Topic name is not a valid SQL identifier");
        }

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            String sql = String.format("""
                    CREATE TABLE IF NOT EXISTS postevent.%s (
                        idn bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        id VARCHAR(255) NOT NULL,
                        source VARCHAR(1024) NOT NULL,
                        type VARCHAR(255) NOT NULL,
                        datacontenttype VARCHAR(255),
                        dataschema VARCHAR(255),
                        subject VARCHAR(255),
                        data bytea,
                        time TIMESTAMP WITH TIME ZONE default current_timestamp,
                        UNIQUE (id, source)
                    )""", topic);

            stmt.execute(sql);
            LOGGER.info("Table creation completed successfully for topic: " + topic);

        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error creating table for topic: " + topic, e);
            throw new RuntimeException("Failed to create table", e);
        }
        return this;
    }

    public DatabaseSetup createMessagesTableIfNotExists() {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            String sql = """
                    CREATE TABLE IF NOT EXISTS postevent.messages (
                        idn bigint PRIMARY KEY NOT NULL,
                        id VARCHAR(255),
                        source VARCHAR(1024),
                        type VARCHAR(255) NOT NULL,
                        datacontenttype VARCHAR(255),
                        dataschema VARCHAR(255),
                        subject VARCHAR(255),
                        data bytea,
                        time TIMESTAMP WITH TIME ZONE,
                        status VARCHAR(1) DEFAULT 'u'
                    )""";

            stmt.execute(sql);
            LOGGER.info("Messages table creation completed successfully");

        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error creating messages table", e);
            throw new RuntimeException("Failed to create messages table", e);
        }
        return this;
    }

    public DatabaseSetup createContiguousHwmTableIfNotExists() {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            String sql = """
                    CREATE TABLE IF NOT EXISTS postevent.contiguous_hwm (
                        subscriber_name VARCHAR(255) PRIMARY KEY,
                        hwm BIGINT NOT NULL
                    )""";

            stmt.execute(sql);
            LOGGER.info("Contiguous HWM table creation completed successfully");

        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error creating contiguous_hwm table", e);
            throw new RuntimeException("Failed to create contiguous_hwm table", e);
        }
        return this;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
}
