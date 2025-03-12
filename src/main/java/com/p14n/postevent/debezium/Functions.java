package com.p14n.postevent.debezium;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.p14n.postevent.data.Event;
import io.debezium.engine.ChangeEvent;

import java.io.IOException;

public class Functions {
    private final static ObjectMapper mapper = new ObjectMapper();

    public static Event changeEventToEvent(ChangeEvent<String, String> record) throws IOException {
        var actualObj = mapper.readTree(record.value());
        var payload = actualObj.get("payload");
        var r = payload != null ? payload.get("after") : null;
        if (r != null) {
            return Event.create(r.get("id").asText(),
                    r.get("source").asText(),
                    r.get("type").asText(),
                    r.get("datacontenttype").asText(),
                    r.get("dataschema").asText(),
                    r.get("subject").asText(),
                    r.get("data").binaryValue());
        }
        return null;
    }
}
