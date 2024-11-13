package com.p14n.postevent;

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

    public void createSchemaIfNotExists() {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            String sql = "CREATE SCHEMA IF NOT EXISTS postevent";
            stmt.execute(sql);
            LOGGER.info("Schema creation completed successfully");
            
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error creating schema", e);
            throw new RuntimeException("Failed to create schema", e);
        }
    }

    public void createTableIfNotExists(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
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
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
}
