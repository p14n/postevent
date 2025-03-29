package com.p14n.postevent.data;

import java.util.Properties;

public record ConfigData(String affinity,
        String topic, // renamed from name
        String dbHost,
        int dbPort,
        String dbUser,
        String dbPassword,
        String dbName,
        Properties overrideProps) implements PostEventConfig {
    public ConfigData(String affinity,
            String topic, // renamed from name
            String dbHost,
            int dbPort,
            String dbUser,
            String dbPassword,
            String dbName) {
        this(affinity, topic, dbHost, dbPort, dbUser, dbPassword, dbName, null);
    }
}
