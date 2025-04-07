package com.p14n.postevent;

import com.p14n.postevent.data.Event;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import org.slf4j.Logger;

public class TestUtil {
    public static Event createTestEvent(int i) {
        return Event.create(
                UUID.randomUUID().toString(),
                "test-source",
                "test-type",
                "application/json",
                null,
                "test-subject",
                ("{\"value\":" + i + "}").getBytes());
    }

    public static Event createTestEvent(int i, String subject) {
        return Event.create(
                UUID.randomUUID().toString(),
                "test-source",
                "test-type",
                "application/json",
                null,
                subject,
                ("{\"value\":" + i + "}").getBytes());
    }

    public static void logEventsInMessagesTable(Connection connection, Logger log) throws Exception {
        log.debug("postevent.messages contents:");
        String sql = "SELECT idn, id, source, status, topic, subject FROM postevent.messages ORDER BY idn";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                log.debug("Event: idn={}, id={}, source={}, status={}, topic={}, subject={}",
                        rs.getLong("idn"),
                        rs.getString("id"),
                        rs.getString("source"),
                        rs.getString("status"),
                        rs.getString("topic"),
                        rs.getString("subject"));
            }
        }
    }

    public static void logEventsInTopicTable(Connection connection, Logger log, String topic) throws Exception {
        log.debug("postevent." + topic + " contents:");
        String sql = "SELECT idn, id, source FROM postevent." + topic + " ORDER BY idn";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                log.debug("Event: idn={}, id={}, source={}",
                        rs.getLong("idn"),
                        rs.getString("id"),
                        rs.getString("source"));
            }
        }
    }

    public static void logEventsInHwmTable(Connection connection, Logger log) throws Exception {
        log.debug("postevent.contiguous_hwm contents:");
        String sql = "SELECT * FROM postevent.contiguous_hwm ORDER BY topic_name";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                log.debug("HWM: topic={}, hwm={}",
                        rs.getString("topic_name"),
                        rs.getLong("hwm"));
            }
        }
    }

}
