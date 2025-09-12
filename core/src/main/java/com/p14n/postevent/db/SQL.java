package com.p14n.postevent.db;

import com.p14n.postevent.data.Event;
import java.sql.*;

/**
 * Utility class providing SQL-related constants and helper methods for database
 * operations.
 * This class handles common database operations such as mapping result sets to
 * events,
 * setting prepared statement parameters, and managing database resources.
 *
 * <p>
 * The class defines two sets of columns:
 * </p>
 * <ul>
 * <li>Core columns: Basic event properties (id, source, type, etc.)</li>
 * <li>Extended columns: Core columns plus additional fields (time, idn,
 * topic)</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * // Using core columns in a SELECT statement
 * String query = "SELECT " + SQL.CORE_COLS + " FROM events";
 *
 * // Setting event data on a prepared statement
 * PreparedStatement stmt = connection.prepareStatement(
 *         "INSERT INTO events (" + SQL.CORE_COLS + ") VALUES (" + SQL.CORE_PH + ")");
 * SQL.setEventOnStatement(stmt, event);
 * }</pre>
 */
public class SQL {

    /** Private constructor to prevent instantiation of utility class */
    private SQL() {
    }

    /** Column names for core event properties */
    public static String CORE_COLS = "id, source, type, datacontenttype, dataschema, subject, data, traceparent";

    /** Column names including both core and extended properties */
    public static String EXT_COLS = CORE_COLS + ", time, idn, topic";

    /** Placeholder parameters for core columns in prepared statements */
    public static String CORE_PH = "?,?,?,?,?,?,?,?";

    /** Placeholder parameters for extended columns in prepared statements */
    public static String EXT_PH = CORE_PH + ",?,?,?";

    /**
     * Creates an Event object from a ResultSet row.
     * Maps database columns to Event properties using the current row data.
     *
     * @param rs    ResultSet positioned at the row to map
     * @param topic Topic name for the event (if not present in ResultSet)
     * @return New Event instance populated with ResultSet data
     * @throws SQLException if any database access error occurs
     */
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
                topic,
                rs.getString("traceparent"));
    }

    /**
     * Sets core event properties on a PreparedStatement.
     * Maps Event properties to the first 8 parameters of the statement.
     *
     * @param stmt  PreparedStatement to set parameters on
     * @param event Event containing the data to set
     * @throws SQLException if any database access error occurs
     */
    public static void setEventOnStatement(PreparedStatement stmt, Event event) throws SQLException {
        stmt.setString(1, event.id());
        stmt.setString(2, event.source());
        stmt.setString(3, event.type());
        stmt.setString(4, event.datacontenttype());
        stmt.setString(5, event.dataschema());
        stmt.setString(6, event.subject());
        stmt.setBytes(7, event.data());
        stmt.setString(8, event.traceparent());
    }

    /**
     * Sets extended event properties (time, IDN, and topic) on a PreparedStatement.
     * Maps these properties to parameters 9-11 of the statement.
     *
     * @param stmt  PreparedStatement to set parameters on
     * @param event Event containing the data to set
     * @throws SQLException if any database access error occurs
     */
    public static void setTimeIDNAndTopic(PreparedStatement stmt, Event event) throws SQLException {
        stmt.setTimestamp(9, Timestamp.from(event.time()));
        stmt.setLong(10, event.idn());
        stmt.setString(11, event.topic());
    }

    /**
     * Safely closes a database connection.
     * Handles null connections and suppresses any exceptions during closure.
     *
     * @param conn Connection to close (may be null)
     */
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

    /**
     * Handles SQLException by attempting to rollback the transaction.
     * If rollback fails, the rollback exception is added as a suppressed exception.
     *
     * @param e    Original SQLException that triggered the rollback
     * @param conn Connection to rollback (may be null)
     */
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
