package com.p14n.postevent.db;

import com.p14n.postevent.data.PostEventConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

public class DatabaseSetup {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseSetup.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public DatabaseSetup(PostEventConfig cfg) {
        this(cfg.jdbcUrl(), cfg.dbUser(), cfg.dbPassword());
    }

    public DatabaseSetup(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public DatabaseSetup setupAll(Set<String> topics) {
        createSchemaIfNotExists();
        createMessagesTableIfNotExists();
        createContiguousHwmTableIfNotExists();
        topics.stream().forEach(this::createTableIfNotExists);
        return this;
    }

    public DatabaseSetup setupAll(String topic) {
        return setupAll(Set.of(topic));
    }

    public DatabaseSetup createSchemaIfNotExists() {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            String sql = "CREATE SCHEMA IF NOT EXISTS postevent";
            stmt.execute(sql);
            logger.atInfo().log("Schema creation completed successfully");

        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error creating schema");
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
            logger.atInfo().log("Table creation completed successfully for topic: {}", topic);

        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error creating table for topic: {}", topic);
            throw new RuntimeException("Failed to create table", e);
        }
        return this;
    }

    public DatabaseSetup createMessagesTableIfNotExists() {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            String sql = """
                    CREATE TABLE IF NOT EXISTS postevent.messages (
                        idn bigint NOT NULL,
                        topic VARCHAR(255) NOT NULL,
                        id VARCHAR(255),
                        source VARCHAR(1024),
                        type VARCHAR(255) NOT NULL,
                        datacontenttype VARCHAR(255),
                        dataschema VARCHAR(255),
                        subject VARCHAR(255),
                        data bytea,
                        time TIMESTAMP WITH TIME ZONE,
                        status VARCHAR(1) DEFAULT 'u',
                        PRIMARY KEY (topic,idn)
                    )""";

            stmt.execute(sql);
            logger.atInfo().log("Messages table creation completed successfully");

        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error creating messages table");
            throw new RuntimeException("Failed to create messages table", e);
        }
        return this;
    }

    public DatabaseSetup createContiguousHwmTableIfNotExists() {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            String sql = """
                    CREATE TABLE IF NOT EXISTS postevent.contiguous_hwm (
                        topic_name VARCHAR(255) PRIMARY KEY,
                        hwm BIGINT NOT NULL
                    )""";

            stmt.execute(sql);
            logger.atInfo().log("Contiguous HWM table creation completed successfully");

        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error creating contiguous HWM table");
            throw new RuntimeException("Failed to create contiguous_hwm table", e);
        }
        return this;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    public static DataSource createPool(PostEventConfig cfg) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(cfg.jdbcUrl());
        ds.setUsername(cfg.dbUser());
        ds.setPassword(cfg.dbPassword());
        return ds;
    }
}
