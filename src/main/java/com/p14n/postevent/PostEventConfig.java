package com.p14n.postevent;

import java.util.Properties;

public interface PostEventConfig {
    public String affinity();

    public String name();

    public String dbHost();

    public int dbPort();

    public String dbUser();

    public String dbPassword();

    public String dbName();

    public Properties overrideProps();

    public default int startupTimeoutSeconds() {
        return 30;
    }
}
