package com.p14n.postevent.data;

import java.util.Properties;
import java.util.Set;

/**
 * Configuration interface for PostEvent system settings.
 * Defines the essential configuration parameters needed for event processing
 * and database connectivity.
 */
public interface PostEventConfig {
    /**
     * Gets the affinity identifier for this instance.
     * The affinity is used to identify related event processors.
     *
     * @return The affinity string identifier
     */
    String affinity();

    /**
     * Gets the set of topics this instance will handle.
     *
     * @return Set of topic names
     */
    Set<String> topics();

    /**
     * Gets the database host address.
     *
     * @return The database host address
     */
    String dbHost();

    /**
     * Gets the database port number.
     *
     * @return The database port number
     */
    int dbPort();

    /**
     * Gets the database username.
     *
     * @return The database username
     */
    String dbUser();

    /**
     * Gets the database password.
     *
     * @return The database password
     */
    String dbPassword();

    /**
     * Gets the database name.
     *
     * @return The database name
     */
    String dbName();

    /**
     * Gets additional database properties for overriding defaults.
     *
     * @return Properties object containing override values
     */
    Properties overrideProps();

    /**
     * Gets the startup timeout in seconds.
     * Default is 30 seconds.
     *
     * @return The startup timeout in seconds
     */
    default int startupTimeoutSeconds() {
        return 30;
    }

    /**
     * Constructs the JDBC URL for database connection.
     *
     * @return The complete JDBC URL string
     */
    default String jdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s",
                dbHost(), dbPort(), dbName());
    }

    /**
     * Gets the poll interval for checking new events.
     *
     * @return The poll interval in milliseconds
     */
    int pollInterval();
}
