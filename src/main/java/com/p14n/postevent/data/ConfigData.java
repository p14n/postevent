package com.p14n.postevent.data;

import java.util.Properties;

public record ConfigData(String affinity,
        String name,
        String dbHost,
        int dbPort,
        String dbUser,
        String dbPassword,
        String dbName,
        Properties overrideProps) implements PostEventConfig {
    public ConfigData(String affinity,
                      String name,
                      String dbHost,
                      int dbPort,
                      String dbUser,
                      String dbPassword,
                      String dbName){
        this(affinity,name,dbHost,dbPort,dbUser,dbPassword,dbName,null);

    }
}
