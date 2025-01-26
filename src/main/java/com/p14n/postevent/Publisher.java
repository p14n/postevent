package com.p14n.postevent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
     * @throws IllegalArgumentException if the topic is null or empty
     */
    public void publish(Event event, Connection connection, String topic) throws SQLException {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }

        String sql = String.format("""
                INSERT INTO postevent.%s
                (id, source, type, datacontenttype, dataschema, subject, data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, topic);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, event.id());
            stmt.setString(2, event.source());
            stmt.setString(3, event.type());
            stmt.setString(4, event.datacontenttype());
            stmt.setString(5, event.dataschema());
            stmt.setString(6, event.subject());
            stmt.setBytes(7, event.data());

            stmt.executeUpdate();
        }
    }
}