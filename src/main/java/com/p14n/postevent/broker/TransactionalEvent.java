package com.p14n.postevent.broker;

import com.p14n.postevent.data.Event;

import java.sql.Connection;

public record TransactionalEvent(Connection connection, Event event){}
