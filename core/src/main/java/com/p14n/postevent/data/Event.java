package com.p14n.postevent.data;

import java.time.Instant;

/**
 * Record representing an event to be published to the database.
 * This immutable class encapsulates all the information needed for event
 * processing
 * and persistence.
 *
 * <p>
 * An event consists of:
 * </p>
 * <ul>
 * <li>Mandatory fields: id, source, type</li>
 * <li>Optional metadata: datacontenttype, dataschema, subject, traceparent</li>
 * <li>Payload: data as byte array</li>
 * <li>System fields: time, idn (sequence number), topic</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * Event event = Event.create(
 *         "event-123",
 *         "order-service",
 *         "order.created",
 *         "application/json",
 *         "order-schema-v1",
 *         "orders",
 *         jsonData.getBytes(),
 *         null);
 * }</pre>
 * 
 * @param id              Unique identifier for the event
 * @param source          System that generated the event
 * @param type            Event type identifier
 * @param datacontenttype MIME type of the data payload
 * @param dataschema      Schema identifier for the data payload
 * @param subject         Subject or category of the event
 * @param data            Event payload as byte array
 * @param time            Timestamp when the event occurred
 * @param idn             Sequential identifier number
 * @param topic           Topic or stream identifier
 * @param traceparent     OpenTelemetry trace parent identifier
 */
public record Event(
        String id,
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
     * This is a convenience method that sets time, idn, and topic to null.
     *
     * @param id              Unique identifier for the event
     * @param source          System that generated the event
     * @param type            Event type identifier
     * @param datacontenttype MIME type of the data payload
     * @param dataschema      Schema identifier for the data payload
     * @param subject         Subject or category of the event
     * @param data            Event payload as byte array
     * @param traceparent     OpenTelemetry trace parent identifier
     * @return A new Event instance
     * @throws IllegalArgumentException if any required field is null or empty
     */
    public static Event create(String id, String source, String type, String datacontenttype, String dataschema,
            String subject, byte[] data, String traceparent) {
        return create(id, source, type, datacontenttype, dataschema, subject, data, null, null, null, traceparent);
    }

    /**
     * Creates a new Event instance with validation of required fields and all
     * optional fields.
     *
     * @param id              Unique identifier for the event
     * @param source          System that generated the event
     * @param type            Event type identifier
     * @param datacontenttype MIME type of the data payload
     * @param dataschema      Schema identifier for the data payload
     * @param subject         Subject or category of the event
     * @param data            Event payload as byte array
     * @param time            Timestamp when the event occurred
     * @param idn             Sequential identifier number
     * @param topic           Topic or stream identifier
     * @param traceparent     OpenTelemetry trace parent identifier
     * @return A new Event instance
     * @throws IllegalArgumentException if id, source, or type is null or empty
     */
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
    public  Event withIdn(Long idn){
        return new Event(id, source, type, datacontenttype, dataschema, subject, data, time, idn, topic, traceparent);
    }
}
