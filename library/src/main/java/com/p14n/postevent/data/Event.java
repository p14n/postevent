package com.p14n.postevent.data;

import java.time.Instant;

/**
 * Record representing an event to be published to the database.
 */
public record Event(String id,
                    String source,
                    String type,
                    String datacontenttype,
                    String dataschema,
                    String subject,
                    byte[] data,
                    Instant time,
                    Long idn,
                    String topic,
                    String traceparent) implements Traceable {

    /**
     * Creates a new Event instance with validation of required fields.
     *
     * @throws IllegalArgumentException if any required field is null or empty
     */
    public static Event create(String id, String source, String type, String datacontenttype, String dataschema,
            String subject, byte[] data, String traceparent) {
        return create(id, source, type, datacontenttype, dataschema, subject, data, null, null, null, traceparent);
    }

    public static Event create(String id, String source, String type, String datacontenttype, String dataschema,
            String subject, byte[] data, Instant time, Long idn, String topic, String traceparent) {
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("id cannot be null or empty");
        }
        if (source == null || source.trim().isEmpty()) {
            throw new IllegalArgumentException("source cannot be null or empty");
        }
        if (type == null || type.trim().isEmpty()) {
            throw new IllegalArgumentException("type cannot be null or empty");
        }

        return new Event(id, source, type, datacontenttype, dataschema, subject, data, time, idn, topic, traceparent);
    }
}
