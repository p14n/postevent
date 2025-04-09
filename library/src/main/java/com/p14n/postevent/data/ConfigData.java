package com.p14n.postevent.data;

import java.util.Properties;
import java.util.Set;

public record ConfigData(String affinity,
        Set<String> topics, // renamed from name
        String dbHost,
        int dbPort,
        String dbUser,
        String dbPassword,
        String dbName,
        Properties overrideProps) implements PostEventConfig {
    public ConfigData(String affinity,
            Set<String> topics, // renamed from name
            String dbHost,
            int dbPort,
            String dbUser,
            String dbPassword,
            String dbName) {
        this(affinity, topics, dbHost, dbPort, dbUser, dbPassword, dbName, null);
    }
}
