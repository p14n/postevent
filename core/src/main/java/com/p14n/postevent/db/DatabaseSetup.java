package com.p14n.postevent.db;

import com.p14n.postevent.data.PostEventConfig;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Set;

/**
 * Handles PostgreSQL database setup and initialization for the PostEvent
 * system.
 * This class manages schema creation, table initialization, and replication
 * slot cleanup.
 *
 * <p>
 * Key responsibilities include:
 * <ul>
 * <li>Creating the PostEvent schema</li>
 * <li>Initializing topic-specific tables</li>
 * <li>Setting up message tracking tables</li>
 * <li>Managing replication slots</li>
 * <li>Providing connection pool setup</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>
 * {@code
 * PostEventConfig config = // initialize configuration
 * DatabaseSetup setup = new DatabaseSetup(config);
 * 
 * // Setup all required tables for given topics
 * setup.setupAll(Set.of("orders", "inventory"));
 * 
 */
public class DatabaseSetup {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseSetup.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private final DataSource ds;

    /**
     * Creates a new DatabaseSetup instance using configuration from
     * PostEventConfig.
     *
     * @param cfg Configuration containing database connection details
     */
    public DatabaseSetup(PostEventConfig cfg) {
        this(cfg.jdbcUrl(), cfg.dbUser(), cfg.dbPassword());
    }

    /**
     * Creates a new DatabaseSetup instance with explicit connection parameters.
     *
     * @param jdbcUrl  PostgreSQL JDBC URL
     * @param username Database username
     * @param password Database password
     */
    public DatabaseSetup(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.ds = null;
    }

    public DatabaseSetup(DataSource ds) {
        this.jdbcUrl = null;
        this.username = null;
        this.password = null;
        this.ds = ds;
    }

    /**
     * Performs complete database setup for multiple topics.
     * Creates schema, message tables, and topic-specific tables.
     *
     * @param topics Set of topic names to initialize
     * @return this instance for method chaining
     * @throws RuntimeException if database operations fail
     */
    public DatabaseSetup setupAll(Set<String> topics) {
        setupClient();
        setupServer(topics);
        setupDebezium();
        return this;
    }

    public DatabaseSetup setupDebezium() {
        clearOldSlots();
        return this;
    }

    public DatabaseSetup setupServer(Set<String> topics) {
        createSchemaIfNotExists();
        topics.stream().forEach(this::createTableIfNotExists);
        return this;
    }

    public DatabaseSetup setupClient() {
        createSchemaIfNotExists();
        createMessagesTableIfNotExists();
        createContiguousHwmTableIfNotExists();
        return this;
    }

    /**
     * Performs complete database setup for a single topic.
     *
     * @param topic Topic name to initialize
     * @return this instance for method chaining
     * @throws RuntimeException if database operations fail
     */
    public DatabaseSetup setupAll(String topic) {
        return setupAll(Set.of(topic));
    }

    /**
     * Removes inactive replication slots with names starting with 'postevent'.
     * This cleanup prevents accumulation of unused slots.
     *
     * @return this instance for method chaining
     */
    public DatabaseSetup clearOldSlots() {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            String sql = "select slot_name FROM pg_replication_slots where active=false and slot_name like 'postevent%'";
            var rs = stmt.executeQuery(sql);
            logger.atInfo().log("Clearing old slots");
            var slotNames = new ArrayList<String>();
            while (rs.next()) {
                slotNames.add(rs.getString(1));
            }
            if (slotNames.isEmpty()) {
                logger.atInfo().log("No old slots to clear");
                return this;
            }
            for (var slotName : slotNames) {
                try (var call = conn.prepareCall("select pg_drop_replication_slot(?)")) {
                    call.setString(1, slotName);
                    call.execute();
                }
            }
        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error clearing old slots");
        }
        return this;
    }

    /**
     * Creates the PostEvent schema if it doesn't exist.
     *
     * @return this instance for method chaining
     * @throws RuntimeException if schema creation fails
     */
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

    /**
     * Creates a topic-specific table if it doesn't exist.
     * The table stores event data with unique constraints on ID and source.
     *
     * @param topic Name of the topic table to create
     * @return this instance for method chaining
     * @throws IllegalArgumentException if topic name is invalid
     * @throws RuntimeException         if table creation fails
     */
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
                        traceparent VARCHAR(55),
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

    /**
     * Creates the messages table if it doesn't exist.
     * This table stores all events with their processing status and supports
     * efficient querying.
     *
     * @return this instance for method chaining
     * @throws RuntimeException if table creation fails
     */
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
                        traceparent VARCHAR(55),
                        PRIMARY KEY (topic,idn)
                    )""";

            stmt.execute(sql);

            // Index for hasUnprocessedPriorEvents query
            stmt.execute("""
                    CREATE INDEX IF NOT EXISTS idx_messages_subject_topic_idn_status
                    ON postevent.messages (subject, topic, idn, status)""");

            // Index for findUnprocessedEvents query
            stmt.execute("""
                    CREATE INDEX IF NOT EXISTS idx_messages_status_time
                    ON postevent.messages (status, time)""");

            logger.atInfo().log("Messages table and indexes creation completed successfully");

        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error creating messages table");
            throw new RuntimeException("Failed to create messages table", e);
        }
        return this;
    }

    /**
     * Creates the contiguous high-water mark table if it doesn't exist.
     * This table tracks the latest continuously processed message ID for each
     * topic.
     *
     * @return this instance for method chaining
     * @throws RuntimeException if table creation fails
     */
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

    /**
     * Creates a database connection using the configured credentials.
     *
     * @return A new database Connection
     * @throws SQLException if connection fails
     */
    private Connection getConnection() throws SQLException {
        if (ds != null)
            return ds.getConnection();
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

}
