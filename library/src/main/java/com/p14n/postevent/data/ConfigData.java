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
        int pollInterval,
        Properties overrideProps) implements PostEventConfig {
    public ConfigData(String affinity,
                      Set<String> topics, // renamed from name
                      String dbHost,
                      int dbPort,
                      String dbUser,
                      String dbPassword,
                      String dbName,
                      int pollInterval) {
        this(affinity, topics, dbHost, dbPort, dbUser, dbPassword, dbName, pollInterval,null);
    }
    public ConfigData(String affinity,
                      Set<String> topics, // renamed from name
                      String dbHost,
                      int dbPort,
                      String dbUser,
                      String dbPassword,
                      String dbName) {
        this(affinity, topics, dbHost, dbPort, dbUser, dbPassword, dbName, 500,null);
    }
}
