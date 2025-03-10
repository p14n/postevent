package com.p14n.postevent;

import com.p14n.postevent.data.Event;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.p14n.postevent.db.SQL.setEventOnStatement;

/**
 * Publisher class responsible for writing events to a PostgreSQL database.
 */
public class Publisher {

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
    public void publish(Event event, Connection connection, String topic) throws SQLException {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (!topic.matches("^[a-z_]+$")) {
            throw new IllegalArgumentException("Topic name must contain only lowercase letters and underscores");
        }

        String sql = String.format("""
                INSERT INTO postevent.%s
                (id, source, type, datacontenttype, dataschema, subject, data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, topic);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setEventOnStatement(stmt,event);
            stmt.executeUpdate();
        }
    }

}