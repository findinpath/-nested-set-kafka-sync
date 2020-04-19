package com.findinpath.sink.jdbc;

import com.findinpath.sink.Utils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * This class acts as a provider for a {@link Connection} from
 * {@link HikariDataSource}.
 */
public class ConnectionProvider implements AutoCloseable {

    private final HikariDataSource dataSource;

    public ConnectionProvider(String driverClassName,
                              String jdbcUrl,
                              String username,
                              String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setDriverClassName(driverClassName);
        config.setUsername(username);
        config.setPassword(password);
        this.dataSource = new HikariDataSource(config);

    }


    public Connection getConnection(){
        try {
            return dataSource.getConnection();
        }catch(SQLException e){
            Utils.sneakyThrow(e);
            return null;
        }
    }


    @Override
    public void close() throws Exception {
        dataSource.close();
    }
}
