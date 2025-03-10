package com.p14n.postevent.db;

import com.p14n.postevent.data.Event;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class SQL {

    public static String CORE_COLS = "id, source, type, datacontenttype, dataschema, subject, data";
    public static String EXT_COLS = CORE_COLS +", time, idn";
    public static String CORE_PH = "?,?,?,?,?,?,?";
    public static String EXT_PH = CORE_PH+",?,?";

    public static Event eventFromResultSet(ResultSet rs) throws SQLException {
        return new Event(
                rs.getString("id"),
                rs.getString("source"),
                rs.getString("type"),
                rs.getString("datacontenttype"),
                rs.getString("dataschema"),
                rs.getString("subject"),
                rs.getBytes("data"),
                rs.getTimestamp("time").toInstant(),
                rs.getLong("idn"));
    }
    public static void setEventOnStatement(PreparedStatement stmt,Event event) throws SQLException {
        stmt.setString(1, event.id());
        stmt.setString(2, event.source());
        stmt.setString(3, event.type());
        stmt.setString(4, event.datacontenttype());
        stmt.setString(5, event.dataschema());
        stmt.setString(6, event.subject());
        stmt.setBytes(7, event.data());
    }
    public static void setTimeAndIDn(PreparedStatement stmt,Event event) throws SQLException {
        stmt.setTimestamp(8, Timestamp.from(event.time()));
        stmt.setLong(9,event.idn());
    }
}

