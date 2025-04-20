package com.p14n.postevent.telemetry;

import io.opentelemetry.context.propagation.TextMapGetter;

import java.util.Map;

public class MapTextMapGetter implements TextMapGetter<Map<String, String>> {
    @Override
    public String get(Map<String, String> carrier, String key) {
        assert carrier != null;
        return carrier.get(key);
    }

    @Override
    public Iterable<String> keys(Map<String, String> carrier) {
        return carrier.keySet();
    }
}