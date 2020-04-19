package com.findinpath.sink.jdbc;

import com.findinpath.sink.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LogOffsetRepository {
    private static final String UPDATE_LOG_OFFSET_SQL = "UPDATE log_offset " +
            "SET value = ? " +
            "WHERE name = ?";

    private static final Logger LOGGER = LoggerFactory.getLogger(LogOffsetRepository.class);

    private final Connection connection;


    public LogOffsetRepository(Connection connection) {
        this.connection = connection;
    }

    public void saveNestedSetLogOffset(String name, long value) {
        LOGGER.info("Updating the log_offset for name " + name + " to " + value);

        try (PreparedStatement pstmt = connection.prepareStatement(UPDATE_LOG_OFFSET_SQL)) {

            pstmt.setLong(1, value);
            pstmt.setString(2, name);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }
    }
}
