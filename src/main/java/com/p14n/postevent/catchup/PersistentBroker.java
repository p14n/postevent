package com.p14n.postevent.catchup;

import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.db.SQL;

import javax.sql.DataSource;
import java.sql.*;

public class PersistentBroker<OutT> implements MessageBroker<Event, OutT>, AutoCloseable {
    private static final String INSERT_SQL = "INSERT INTO postevent.messages (" + SQL.EXT_COLS +
            ") VALUES (" + SQL.EXT_PH + ")";

    private final MessageBroker<Event, OutT> targetBroker;
    private final DataSource dataSource;

    public PersistentBroker(MessageBroker<Event, OutT> targetBroker, DataSource dataSource) {
        this.targetBroker = targetBroker;
        this.dataSource = dataSource;
    }

    @Override
    public void publish(Event event) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
                SQL.setEventOnStatement(stmt, event);
                SQL.setTimeAndIDn(stmt, event);
                stmt.executeUpdate();
            }
            conn.commit();

            // Forward to actual subscriber after successful persistence
            targetBroker.publish(event);

        } catch (SQLException e) {
            SQL.handleSQLException(e, conn);
            throw new RuntimeException("Failed to persist and forward event", e);
        } finally {
            SQL.closeConnection(conn);
        }
    }

    @Override
    public boolean subscribe(MessageSubscriber<OutT> subscriber) {
        return targetBroker.subscribe(subscriber);
    }

    @Override
    public boolean unsubscribe(MessageSubscriber<OutT> subscriber) {
        return targetBroker.unsubscribe(subscriber);
    }

    @Override
    public void close() {
        targetBroker.close();
    }

    @Override
    public OutT convert(Event m) {
        return null;
    }
}