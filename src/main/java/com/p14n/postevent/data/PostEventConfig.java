package com.p14n.postevent.data;

import java.util.Properties;

public interface PostEventConfig {
    public String affinity();

    public String topic(); // renamed from name()

    public String dbHost();

    public int dbPort();

    public String dbUser();

    public String dbPassword();

    public String dbName();

    public Properties overrideProps();

    public default int startupTimeoutSeconds() {
        return 30;
    }

    public default String jdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s",
                dbHost(), dbPort(), dbName());
    }
}
