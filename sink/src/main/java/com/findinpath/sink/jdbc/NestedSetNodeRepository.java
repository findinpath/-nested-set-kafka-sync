package com.findinpath.sink.jdbc;

import com.findinpath.sink.Utils;
import com.findinpath.sink.model.NestedSetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import static com.findinpath.sink.jdbc.Constants.TZ_UTC;

public class NestedSetNodeRepository {
    private static final String SELECT_NESTED_SET_NODE_SQL =
            "SELECT id, label, lft, rgt, active, created, updated " +
                    "FROM nested_set_node " +
                    "WHERE id = ?";
    private static final String SELECT_NESTED_SET_NODES_SQL =
            "SELECT id, label, lft, rgt, active, created, updated " +
                    "FROM nested_set_node ";
    private static final String INSERT_NESTED_SET_NODE_SQL =
            "INSERT INTO nested_set_node (id, label, lft, rgt, active, created, updated) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_NESTED_SET_NODE_SQL =
            "UPDATE nested_set_node " +
                    "SET label = ?, lft = ?, rgt = ?, active = ?, updated = ? " +
                    "WHERE id = ?";

    private static final Logger LOGGER = LoggerFactory.getLogger(NestedSetNodeRepository.class);

    private final Connection connection;


    public NestedSetNodeRepository(Connection connection) {
        this.connection = connection;
    }

    public Optional<NestedSetNode> getNestedSetNode(long id) {
        try (PreparedStatement pstmt = connection.prepareStatement(SELECT_NESTED_SET_NODE_SQL)) {

            pstmt.setLong(1, id);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    var nestedSetNode = new NestedSetNode();
                    nestedSetNode.setId(rs.getLong(1));
                    nestedSetNode.setLabel(rs.getString(2));
                    nestedSetNode.setLeft(rs.getInt(3));
                    nestedSetNode.setRight(rs.getInt(4));
                    nestedSetNode.setActive(rs.getBoolean(5));
                    nestedSetNode.setCreated(rs.getTimestamp(6, TZ_UTC).toInstant());
                    nestedSetNode.setUpdated(rs.getTimestamp(7, TZ_UTC).toInstant());
                    return Optional.of(nestedSetNode);
                }
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }

        return Optional.empty();
    }

    public List<NestedSetNode> getNestedSetNodes() {
        try (PreparedStatement pstmt = connection.prepareStatement(SELECT_NESTED_SET_NODES_SQL);
             ResultSet rs = pstmt.executeQuery()) {
            var result = new ArrayList<NestedSetNode>();
            while (rs.next()) {
                var nestedSetNode = new NestedSetNode();
                nestedSetNode.setId(rs.getInt(1));
                nestedSetNode.setLabel(rs.getString(2));
                nestedSetNode.setLeft(rs.getInt(3));
                nestedSetNode.setRight(rs.getInt(4));
                nestedSetNode.setActive(rs.getBoolean(5));
                nestedSetNode.setCreated(rs.getTimestamp(6, TZ_UTC).toInstant());
                nestedSetNode.setUpdated(rs.getTimestamp(7, TZ_UTC).toInstant());
                result.add(nestedSetNode);
            }

            return result;
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return null;
        }
    }

    public void insertAll(Iterable<NestedSetNode> nestedSetNodes) {
        LOGGER.info("Inserting new values in the nested_set_node table");

        try (PreparedStatement pstmt = connection.prepareStatement(INSERT_NESTED_SET_NODE_SQL)) {

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

    public void updateAll(Iterable<NestedSetNode> nestedSetNodes) {
        LOGGER.info("Updating the nested_set_node table");

        try (PreparedStatement pstmt = connection.prepareStatement(UPDATE_NESTED_SET_NODE_SQL)) {

            for (var nestedSetNode : nestedSetNodes) {
                pstmt.setString(1, nestedSetNode.getLabel());
                pstmt.setInt(2, nestedSetNode.getLeft());
                pstmt.setInt(3, nestedSetNode.getRight());
                pstmt.setBoolean(4, nestedSetNode.isActive());
                pstmt.setTimestamp(5, new Timestamp(nestedSetNode.getUpdated().toEpochMilli()), TZ_UTC);
                pstmt.setLong(6, nestedSetNode.getId());
                pstmt.addBatch();
            }

            pstmt.executeBatch();
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }
    }
}
