package com.p14n.postevent;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.SQL;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.p14n.postevent.db.SQL.setEventOnStatement;

/**
 * Publisher class responsible for writing events to a PostgreSQL database.
 */
public class Publisher {

    private Publisher(){

    }
    /**
     * Publishes an event to the specified topic table.
     *
     * @param event      The event to publish
     * @param connection The database connection
     * @param topic      The topic/table name to publish to
     * @throws SQLException             if a database access error occurs
     * @throws IllegalArgumentException if the topic is null, empty, or contains
     *                                  invalid characters
     */
    public static void publish(Event event, Connection connection, String topic) throws SQLException {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (!topic.matches("^[a-z_]+$")) {
            throw new IllegalArgumentException("Topic name must contain only lowercase letters and underscores");
        }

        String sql = String.format("INSERT INTO postevent.%s (%s) VALUES (%s)",
                topic, SQL.CORE_COLS, SQL.CORE_PH);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setEventOnStatement(stmt, event);
            stmt.executeUpdate();
        }
    }

    public static void publish(Event event, DataSource ds, String topic) throws SQLException {
        try (Connection c = ds.getConnection()) {
            publish(event, c, topic);
        }
    }
}