package com.p14n.postevent.telemetry;

import io.opentelemetry.context.propagation.TextMapGetter;

import java.util.Map;

/**
 * Implementation of OpenTelemetry's TextMapGetter interface for extracting
 * context from a Map.
 * This class enables the extraction of trace context information from a
 * String-to-String Map carrier,
 * which is commonly used in distributed tracing scenarios.
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>{@code
 * Map<String, String> carrier = new HashMap<>();
 * carrier.put("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
 * 
 * MapTextMapGetter getter = new MapTextMapGetter();
 * String traceParent = getter.get(carrier, "traceparent");
 * }</pre>
 */
public class MapTextMapGetter implements TextMapGetter<Map<String, String>> {

    /** Private constructor to prevent instantiation of utility class */
    public MapTextMapGetter() {
    }

    /**
     * Retrieves a value from the carrier Map using the specified key.
     *
     * @param carrier The Map containing the context information
     * @param key     The key whose value should be retrieved
     * @return The value associated with the key, or null if not present
     * @throws AssertionError if the carrier is null
     */
    @Override
    public String get(Map<String, String> carrier, String key) {
        assert carrier != null;
        return carrier.get(key);
    }

    /**
     * Returns all keys from the carrier Map.
     *
     * @param carrier The Map containing the context information
     * @return An Iterable of all keys in the carrier
     */
    @Override
    public Iterable<String> keys(Map<String, String> carrier) {
        return carrier.keySet();
    }
}
