package com.p14n.postevent.data;

public interface Traceable {
    public String id();

    public String topic();

    public String subject();

    public String traceparent();
}
