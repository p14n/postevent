package com.p14n.postevent.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.p14n.postevent.data.Event;
import io.debezium.engine.ChangeEvent;

import java.io.IOException;
import java.time.Instant;

/**
 * Utility class providing functions for converting Debezium change events into
 * application Events.
 * Handles the parsing and transformation of CDC (Change Data Capture) events
 * from
 * Debezium into the application's event model.
 *
 * <p>
 * The class expects Debezium events in JSON format with a specific structure
 * containing:
 * <ul>
 * <li>payload.after - The new state of the record</li>
 * <li>payload.source.table - The source table name</li>
 * <li>Various event metadata fields (id, source, type, etc.)</li>
 * </ul>
 */
public class Functions {

    /** Private constructor to prevent instantiation of utility class */
    private Functions() {
    }

    /** JSON ObjectMapper instance for parsing Debezium event payloads */
    private final static ObjectMapper mapper = new ObjectMapper();

    /**
     * Safely extracts text from a JsonNode, returning null if the node is null.
     *
     * @param j The JsonNode to extract text from
     * @return The text value of the node, or null if the node is null
     */
    private static String safeText(JsonNode j) {
        if (j != null) {
            return j.asText();
        }
        return null;
    }

    /**
     * Converts a Debezium ChangeEvent into an application Event.
     * Parses the JSON payload and extracts relevant fields to construct a new Event
     * instance.
     *
     * <p>
     * The method expects the following fields in the change event payload:
     * <ul>
     * <li>id - Event identifier</li>
     * <li>source - Event source</li>
     * <li>type - Event type</li>
     * <li>datacontenttype - Content type of the data</li>
     * <li>dataschema - Schema of the data</li>
     * <li>subject - Event subject</li>
     * <li>data - Binary event data</li>
     * <li>time - Event timestamp</li>
     * <li>idn - Sequential identifier</li>
     * <li>traceparent - OpenTelemetry trace parent</li>
     * </ul>
     *
     * @param record The Debezium ChangeEvent to convert
     * @return A new Event instance, or null if the record doesn't contain required
     *         fields
     * @throws IOException If there's an error parsing the JSON or extracting binary
     *                     data
     */
    public static Event changeEventToEvent(ChangeEvent<String, String> record) throws IOException {
        var actualObj = mapper.readTree(record.value());
        var payload = actualObj.get("payload");
        var r = payload != null ? payload.get("after") : null;
        if (r != null && payload != null &&
                payload.get("source") != null &&
                payload.get("source").get("table") != null) {
            var topic = payload.get("source").get("table");
            return Event.create(safeText(r.get("id")),
                    safeText(r.get("source")),
                    safeText(r.get("type")),
                    safeText(r.get("datacontenttype")),
                    safeText(r.get("dataschema")),
                    safeText(r.get("subject")),
                    r.get("data").binaryValue(),
                    Instant.parse(r.get("time").asText()),
                    r.get("idn").asLong(),
                    topic.asText(),
                    safeText(r.get("traceparent")));
        }
        return null;
    }
}
