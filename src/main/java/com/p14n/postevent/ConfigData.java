package com.p14n.postevent;

import java.util.Properties;

public record ConfigData(String affinity,
        String name,
        String dbHost,
        int dbPort,
        String dbUser,
        String dbPassword,
        String dbName,
        Properties overrideProps) implements PostEventConfig {
}
