package com.p14n.postevent;

import com.p14n.postevent.data.Event;

import java.util.UUID;

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
}
