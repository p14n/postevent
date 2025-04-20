package com.p14n.postevent.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.p14n.postevent.data.Event;
import io.debezium.engine.ChangeEvent;

import java.io.IOException;
import java.time.Instant;

public class Functions {
    private final static ObjectMapper mapper = new ObjectMapper();

    private static String safeText(JsonNode j) {
        if(j != null){
            return j.asText();
        }
        return null;
    }
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
