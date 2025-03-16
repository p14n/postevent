package com.p14n.postevent.catchup;

import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.SQL;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CatchupServer implements CatchupServerInterface {
    private static final Logger LOGGER = Logger.getLogger(CatchupServer.class.getName());
    private final String topic;
    private final DataSource dataSource;

    public CatchupServer(String topic, DataSource dataSource) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        this.topic = topic;
        this.dataSource = dataSource;
    }

    @Override
    public List<Event> fetchEvents(long startAfter, long end, int maxResults) {
        if (startAfter > end) {
            throw new IllegalArgumentException("Start value must be less than or equal to end value");
        }
        if (maxResults <= 0) {
            throw new IllegalArgumentException("Max results must be greater than zero");
        }

        List<Event> events = new ArrayList<>();
        String sql = String.format(
                "SELECT * FROM postevent.%s WHERE idn BETWEEN (?+1) AND ? ORDER BY idn LIMIT ?",
                topic);

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, startAfter);
            stmt.setLong(2, end);
            stmt.setInt(3, maxResults);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Event event = SQL.eventFromResultSet(rs, topic);
                    events.add(event);
                }
            }

            LOGGER.info(String.format("Fetched %d events from topic %s between %d and %d",
                    events.size(), topic, startAfter, end));

            return events;

        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error fetching events from database", e);
            throw new RuntimeException("Failed to fetch events", e);
        }
    }
}