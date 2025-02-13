package com.p14n.postevent;

import javax.sql.DataSource;
import java.sql.*;

public class PersistentSubscriber implements MessageSubscriber<Event> {
    private static final String INSERT_SQL = """
            INSERT INTO postevent.messages (
                id, source, datacontenttype, dataschema, subject, data, time
            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """;

    private final MessageSubscriber<Event> targetSubscriber;
    private final DataSource dataSource;

    public PersistentSubscriber(MessageSubscriber<Event> targetSubscriber, DataSource dataSource) {
        this.targetSubscriber = targetSubscriber;
        this.dataSource = dataSource;
    }

    @Override
    public void onMessage(Event event) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
                stmt.setString(1, event.id());
                stmt.setString(2, event.source());
                stmt.setString(3, event.datacontenttype());
                stmt.setString(4, event.dataschema());
                stmt.setString(5, event.subject());
                stmt.setBytes(6, event.data());

                stmt.executeUpdate();
            }
            conn.commit();

            // Forward to actual subscriber after successful persistence
            targetSubscriber.onMessage(event);

        } catch (SQLException e) {
            if (conn != null) {
                try {
                    if (!conn.isClosed()) {
                        conn.rollback();
                    }
                } catch (SQLException rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
            }
            throw new RuntimeException("Failed to persist and forward event", e);
        } finally {
            if (conn != null) {
                try {
                    if (!conn.isClosed()) {
                        conn.close();
                    }
                } catch (SQLException closeEx) {
                    // Optionally log the closing exception
                }
            }
        }
    }

    @Override
    public void onError(Throwable error) {
        targetSubscriber.onError(error);
    }
}