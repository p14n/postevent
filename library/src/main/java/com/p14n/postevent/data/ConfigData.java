package com.p14n.postevent.data;

import java.util.Properties;
import java.util.Set;

/**
 * Implementation of PostEventConfig that provides configuration data
 * for the PostEvent system.
 * 
 * <p>
 * This class holds all necessary configuration parameters for connecting
 * to the database and processing events.
 * </p>
 * 
 * @param affinity      The affinity identifier for this instance
 * @param topics        Set of topics to handle
 * @param dbHost        Database host address
 * @param dbPort        Database port number
 * @param dbUser        Database username
 * @param dbPassword    Database password
 * @param dbName        Database name
 * @param pollInterval  Poll interval in milliseconds
 * @param overrideProps Additional database properties for overriding debezium
 *                      defaults
 */
public record ConfigData(String affinity,
        Set<String> topics, // renamed from name
        String dbHost,
        int dbPort,
        String dbUser,
        String dbPassword,
        String dbName,
        int pollInterval,
        Properties overrideProps) implements PostEventConfig {

    /**
     * Creates a new ConfigData instance with the specified parameters.
     *
     * @param affinity     The affinity identifier for this instance
     * @param topics       Set of topics to handle
     * @param dbHost       Database host address
     * @param dbPort       Database port number
     * @param dbUser       Database username
     * @param dbPassword   Database password
     * @param dbName       Database name
     * @param pollInterval Poll interval in milliseconds
     */
    public ConfigData(String affinity,
            Set<String> topics,
            String dbHost,
            int dbPort,
            String dbUser,
            String dbPassword,
            String dbName,
            int pollInterval) {
        this(affinity, topics, dbHost, dbPort, dbUser, dbPassword, dbName, pollInterval, null);
    }

    /**
     * Creates a new ConfigData instance with the specified parameters.
     *
     * @param affinity   The affinity identifier for this instance
     * @param topics     Set of topics to handle
     * @param dbHost     Database host address
     * @param dbPort     Database port number
     * @param dbUser     Database username
     * @param dbPassword Database password
     * @param dbName     Database name
     */
    public ConfigData(String affinity,
            Set<String> topics, // renamed from name
            String dbHost,
            int dbPort,
            String dbUser,
            String dbPassword,
            String dbName) {
        this(affinity, topics, dbHost, dbPort, dbUser, dbPassword, dbName, 500, null);
    }
}
