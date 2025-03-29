package com.p14n.postevent.catchup;

import com.p14n.postevent.ConsumerServer;
import com.p14n.postevent.data.Event;
import com.p14n.postevent.db.SQL;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CatchupServer implements CatchupServerInterface {
    private static final Logger logger = LoggerFactory.getLogger(CatchupServer.class);

    private final DataSource dataSource;

    public CatchupServer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<Event> fetchEvents(long startAfter, long end, int maxResults, String topic) {
        if (startAfter > end) {
            throw new IllegalArgumentException("Start value must be less than or equal to end value");
        }
        if (maxResults <= 0) {
            throw new IllegalArgumentException("Max results must be greater than zero");
        }
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
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

            logger.atInfo().log(String.format("Fetched %d events from topic %s between %d and %d",
                    events.size(), topic, startAfter, end));

            return events;

        } catch (SQLException e) {
            logger.atError().setCause(e).log("Error fetching events from database");
            throw new RuntimeException("Failed to fetch events", e);
        }
    }
}