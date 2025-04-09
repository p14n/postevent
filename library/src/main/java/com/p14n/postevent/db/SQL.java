package com.p14n.postevent.db;

import com.p14n.postevent.data.Event;

import java.sql.*;

public class SQL {

    public static String CORE_COLS = "id, source, type, datacontenttype, dataschema, subject, data";
    public static String EXT_COLS = CORE_COLS + ", time, idn, topic";
    public static String CORE_PH = "?,?,?,?,?,?,?";
    public static String EXT_PH = CORE_PH + ",?,?,?";

    public static Event eventFromResultSet(ResultSet rs, String topic) throws SQLException {
        return Event.create(
                rs.getString("id"),
                rs.getString("source"),
                rs.getString("type"),
                rs.getString("datacontenttype"),
                rs.getString("dataschema"),
                rs.getString("subject"),
                rs.getBytes("data"),
                rs.getTimestamp("time").toInstant(),
                rs.getLong("idn"),
                topic);
    }

    public static void setEventOnStatement(PreparedStatement stmt, Event event) throws SQLException {
        stmt.setString(1, event.id());
        stmt.setString(2, event.source());
        stmt.setString(3, event.type());
        stmt.setString(4, event.datacontenttype());
        stmt.setString(5, event.dataschema());
        stmt.setString(6, event.subject());
        stmt.setBytes(7, event.data());
    }

    public static void setTimeIDNAndTopic(PreparedStatement stmt, Event event) throws SQLException {
        stmt.setTimestamp(8, Timestamp.from(event.time()));
        stmt.setLong(9, event.idn());
        stmt.setString(10, event.topic());
    }

    public static void closeConnection(Connection conn) {
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

    public static void handleSQLException(SQLException e, Connection conn) {
        if (conn != null) {
            try {
                if (!conn.isClosed()) {
                    conn.rollback();
                }
            } catch (SQLException rollbackEx) {
                e.addSuppressed(rollbackEx);
            }
        }
    }

}
