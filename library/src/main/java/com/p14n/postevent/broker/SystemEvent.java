package com.p14n.postevent.broker;

import com.p14n.postevent.data.Traceable;

public enum SystemEvent implements Traceable {

    CatchupRequired,
    UnprocessedCheckRequired;

    public String topic;

    public SystemEvent withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    @Override
    public String id() {
        return this.toString();
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String subject() {
        return "";
    }

    @Override
    public String traceparent() {
        return null;
    }
}
