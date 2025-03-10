package com.p14n.postevent.catchup;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.db.SQL;

import javax.sql.DataSource;
import java.sql.*;

public class PersistentSubscriber implements MessageSubscriber<Event> {
    private static final String INSERT_SQL =
            "INSERT INTO postevent.messages (" + SQL.EXT_COLS +
                    ") VALUES (" + SQL.EXT_PH + ")";

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
                SQL.setEventOnStatement(stmt,event);
                SQL.setTimeAndIDn(stmt,event);
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