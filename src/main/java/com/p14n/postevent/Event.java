package com.p14n.postevent;

/**
 * Record representing an event to be published to the database.
 */
public record Event(
        String id,
        String source,
        String type,
        String datacontenttype,
        String dataschema,
        String subject,
        byte[] data) {
}