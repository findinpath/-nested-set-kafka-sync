package com.findinpath.sink.jdbc;

import com.findinpath.sink.Utils;
import com.findinpath.sink.model.NestedSetNode;
import com.findinpath.sink.model.NestedSetNodeLog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.findinpath.sink.jdbc.Constants.TZ_UTC;

public class NestedSetNodeLogRepository {
    private static final String SELECT_NESTED_SET_LOG_UPDATES_SQL =
            "SELECT id, tree_node_id, label, lft, rgt, active, created, updated " +
                    "FROM nested_set_node_log " +
                    "WHERE id >= GREATEST((SELECT value FROM log_offset WHERE name = 'nested_set_node_log'), 0)";
    private static final String INSERT_INTO_NESTED_SET_LOG_SQL =
            "INSERT INTO nested_set_node_log (tree_node_id, label, lft, rgt, active, created, updated) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";

    private final Connection connection;

    public NestedSetNodeLogRepository(Connection connection) {
        this.connection = connection;
    }

    public List<NestedSetNodeLog> getNestedSetLogUpdates() {

        try (PreparedStatement pstmt = connection.prepareStatement(SELECT_NESTED_SET_LOG_UPDATES_SQL);
             ResultSet rs = pstmt.executeQuery()) {
            var result = new ArrayList<NestedSetNodeLog>();
            while (rs.next()) {
                var nestedSetLogId = rs.getInt(1);
                var nestedSetNode = new NestedSetNode();
                nestedSetNode.setId(rs.getInt(2));
                nestedSetNode.setLabel(rs.getString(3));
                nestedSetNode.setLeft(rs.getInt(4));
                nestedSetNode.setRight(rs.getInt(5));
                nestedSetNode.setActive(rs.getBoolean(6));
                nestedSetNode.setCreated(rs.getTimestamp(7, TZ_UTC).toInstant());
                nestedSetNode.setUpdated(rs.getTimestamp(8, TZ_UTC).toInstant());
                var nestedSetNodeLog = new NestedSetNodeLog(nestedSetLogId, nestedSetNode);
                result.add(nestedSetNodeLog);
            }

            return result;
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return null;
        }
    }

    public void saveAll(Iterable<NestedSetNode> nestedSetNodes) {

        try (PreparedStatement pstmt = connection.prepareStatement(INSERT_INTO_NESTED_SET_LOG_SQL)) {

            for (var nestedSetNode : nestedSetNodes) {
                pstmt.setLong(1, nestedSetNode.getId());
                pstmt.setString(2, nestedSetNode.getLabel());
                pstmt.setInt(3, nestedSetNode.getLeft());
                pstmt.setInt(4, nestedSetNode.getRight());
                pstmt.setBoolean(5, nestedSetNode.isActive());
                pstmt.setTimestamp(6, new Timestamp(nestedSetNode.getCreated().toEpochMilli()), TZ_UTC);
                pstmt.setTimestamp(7, new Timestamp(nestedSetNode.getUpdated().toEpochMilli()), TZ_UTC);
                pstmt.addBatch();
            }

            pstmt.executeBatch();
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }

    }

}
