package com.p14n.postevent.broker;

public enum SystemEvent {

    CatchupRequired;

    public String topic;

    public SystemEvent withTopic(String topic) {
        this.topic = topic;
        return this;
    }
}
