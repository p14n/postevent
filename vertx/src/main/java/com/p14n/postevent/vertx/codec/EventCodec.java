package com.p14n.postevent.vertx.codec;

import com.p14n.postevent.data.Event;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.Json;

import java.nio.charset.StandardCharsets;

/**
 * MessageCodec for serializing Event objects on the Vert.x EventBus.
 * This codec handles the conversion between Event objects and their wire format
 * for transmission over the EventBus.
 * 
 * <p>
 * The codec uses JSON serialization for simplicity and debugging ease.
 * Events are encoded as JSON strings with a length prefix for efficient
 * parsing.
 * </p>
 * 
 * <p>
 * Wire format:
 * [4 bytes: length][JSON string]
 * </p>
 */
public class EventCodec implements MessageCodec<Event, Event> {

    /**
     * Creates a new EventCodec for serializing Event objects on the Vert.x
     * EventBus.
     */
    public EventCodec() {
        // Default constructor
    }

    /**
     * Encodes an Event object to the wire format.
     * The event is serialized to JSON and prefixed with its length.
     *
     * @param buffer The buffer to write the encoded event to
     * @param event  The event to encode
     */
    @Override
    public void encodeToWire(Buffer buffer, Event event) {
        String json = Json.encode(event);
        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);

        // Write length prefix followed by JSON bytes
        buffer.appendInt(jsonBytes.length);
        buffer.appendBytes(jsonBytes);
    }

    /**
     * Decodes an Event object from the wire format.
     * Reads the length prefix and then deserializes the JSON string.
     *
     * @param pos    The position in the buffer to start reading from
     * @param buffer The buffer containing the encoded event
     * @return The decoded Event object
     */
    @Override
    public Event decodeFromWire(int pos, Buffer buffer) {
        // Read length prefix
        int length = buffer.getInt(pos);

        // Read JSON bytes and convert to string
        byte[] jsonBytes = buffer.getBytes(pos + 4, pos + 4 + length);
        String json = new String(jsonBytes, StandardCharsets.UTF_8);

        // Deserialize from JSON
        return Json.decodeValue(json, Event.class);
    }

    /**
     * Transform method for local delivery.
     * Since we're using the same type for both send and receive,
     * no transformation is needed.
     *
     * @param event The event to transform
     * @return The same event (no transformation)
     */
    @Override
    public Event transform(Event event) {
        return event; // No transformation needed for local delivery
    }

    /**
     * Returns the name of this codec.
     * Used by Vert.x for codec registration and identification.
     *
     * @return The codec name
     */
    @Override
    public String name() {
        return "postevent-event";
    }

    /**
     * Returns the system codec ID.
     * Since this is a user-defined codec, we return -1.
     *
     * @return -1 to indicate this is a user codec
     */
    @Override
    public byte systemCodecID() {
        return -1; // User-defined codec
    }
}
