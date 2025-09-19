package com.p14n.postevent.db;

import com.p14n.postevent.data.PostEventConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class PoolSetup {
    /**
     * Creates and configures a connection pool using HikariCP.
     *
     * @param cfg Configuration containing database connection details
     * @return Configured DataSource
     */
    public static DataSource createPool(PostEventConfig cfg) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(cfg.jdbcUrl());
        ds.setUsername(cfg.dbUser());
        ds.setPassword(cfg.dbPassword());
        return ds;
    }

}
