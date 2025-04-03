package com.p14n.postevent.catchup;

import com.p14n.postevent.broker.MessageBroker;
import com.p14n.postevent.broker.SystemEvent;
import com.p14n.postevent.broker.SystemEventBroker;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.db.SQL;

import javax.sql.DataSource;
import java.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentBroker<OutT> implements MessageBroker<Event, OutT>, AutoCloseable, MessageSubscriber<Event> {
    private static final Logger logger = LoggerFactory.getLogger(PersistentBroker.class);
    private static final String INSERT_SQL = "INSERT INTO postevent.messages (" + SQL.EXT_COLS +
            ") VALUES (" + SQL.EXT_PH + ") ON CONFLICT DO NOTHING";
    private static final String UPDATE_HWM_SQL = "UPDATE postevent.contiguous_hwm set hwm=? where topic_name=? and hwm=?";

    private final MessageBroker<Event, OutT> targetBroker;
    private final DataSource dataSource;
    // private final String topicName;
    private final SystemEventBroker systemEventBroker;

    public PersistentBroker(MessageBroker<Event, OutT> targetBroker,
            DataSource dataSource,
            SystemEventBroker systemEventBroker) {
        this.targetBroker = targetBroker;
        this.dataSource = dataSource;
        this.systemEventBroker = systemEventBroker;
    }

    @Override
    public void publish(String topic, Event event) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
                SQL.setEventOnStatement(stmt, event);
                SQL.setTimeIDNAndTopic(stmt, event);
                stmt.executeUpdate();
            }
            int updates = 0;
            try (PreparedStatement stmt = conn.prepareStatement(UPDATE_HWM_SQL)) {
                stmt.setLong(1, event.idn());
                stmt.setString(2, event.topic());
                stmt.setLong(3, event.idn() - 1);
                updates = stmt.executeUpdate();
                if (updates < 1) {
                    logger.atDebug().log("Publishing catchup required event");
                    systemEventBroker
                            .publish(SystemEvent.CatchupRequired.withTopic(event.topic()));

                }
            }

            conn.commit();

            // Forward to actual subscriber after successful persistence
            if (updates > 0) {
                logger.atDebug().log("Forwarding event to target broker");
                targetBroker.publish(topic, event);
            }

        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error persisting and forwarding event");
            SQL.handleSQLException(e, conn);
            throw new RuntimeException("Failed to persist and forward event", e);
        } finally {
            SQL.closeConnection(conn);
        }
    }

    @Override
    public boolean subscribe(String topic, MessageSubscriber<OutT> subscriber) {
        return targetBroker.subscribe(topic, subscriber);
    }

    @Override
    public boolean unsubscribe(String topic, MessageSubscriber<OutT> subscriber) {
        return targetBroker.unsubscribe(topic, subscriber);
    }

    @Override
    public void close() {
        targetBroker.close();
    }

    @Override
    public OutT convert(Event m) {
        return null;
    }

    @Override
    public void onMessage(Event message) {
        logger.atDebug().log("Received event for persistence");
        publish(message.topic(), message);
    }
}
