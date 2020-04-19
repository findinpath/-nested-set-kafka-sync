package com.findinpath.source.jdbc;


import com.findinpath.source.Utils;
import com.findinpath.source.model.NestedSetNode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

public class NestedSetNodeRepository {

    private static final Calendar TZ_UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private static final String SELECT_NESTED_SET_NODES_SQL =
            "SELECT id, label, lft, rgt, active, created, updated " +
                    "FROM nested_set_node ";
    private static final String SELECT_NESTED_SET_NODE_SQL =
            "SELECT id, label, lft, rgt, active, created, updated " +
                    "FROM nested_set_node " +
                    "WHERE id = ?";
    private static final String INSERT_NESTED_SET_NODE_SQL =
            "INSERT INTO nested_set_node (label, lft, rgt, active, created, updated) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";

    private static final String UPDATE_RIGHT_TO_MAKE_SPACE_FOR_NEW_NODE_SQL =
            "UPDATE nested_set_node SET rgt=rgt+2, updated = ? WHERE rgt>=?;";
    private static final String UPDATE_LEFT_TO_MAKE_SPACE_FOR_NEW_NODE_SQL =
            "UPDATE nested_set_node SET lft=lft+2, updated = ? WHERE lft>?;";

    private static final String SELECT_IS_TABLE_EMPTY =
            "SELECT CASE \n" +
                    "         WHEN EXISTS (SELECT * FROM nested_set_node LIMIT 1) THEN 1\n" +
                    "         ELSE 0 \n" +
                    "       END";

    private final Connection connection;


    public NestedSetNodeRepository(Connection connection) {
        this.connection = connection;
    }

    public List<NestedSetNode> getNestedSetNodes() {
        try (PreparedStatement pstmt = connection.prepareStatement(SELECT_NESTED_SET_NODES_SQL);
             ResultSet rs = pstmt.executeQuery()) {
            var result = new ArrayList<NestedSetNode>();
            while (rs.next()) {
                var nestedSetNode = new NestedSetNode();
                nestedSetNode.setId(rs.getLong(1));
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

    public void makeSpaceForNewNode(int parentNodeRight) {

        try (PreparedStatement pstmt = connection.prepareStatement(UPDATE_RIGHT_TO_MAKE_SPACE_FOR_NEW_NODE_SQL)) {
            pstmt.setTimestamp(1, new Timestamp(Instant.now().toEpochMilli()), TZ_UTC);
            pstmt.setInt(2, parentNodeRight);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }

        try (PreparedStatement pstmt = connection.prepareStatement(UPDATE_LEFT_TO_MAKE_SPACE_FOR_NEW_NODE_SQL)) {
            pstmt.setTimestamp(1, new Timestamp(Instant.now().toEpochMilli()), TZ_UTC);
            pstmt.setInt(2, parentNodeRight);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }

    }

    public long insertNode(String data, int left, int right) {
        try (PreparedStatement pstmt = connection.prepareStatement(INSERT_NESTED_SET_NODE_SQL, Statement.RETURN_GENERATED_KEYS)) {

            pstmt.setString(1, data);
            pstmt.setInt(2, left);
            pstmt.setInt(3, right);
            pstmt.setBoolean(4, true);
            var now = Instant.now();
            pstmt.setTimestamp(5, new Timestamp(now.toEpochMilli()), TZ_UTC);
            pstmt.setTimestamp(6, new Timestamp(now.toEpochMilli()), TZ_UTC);

            pstmt.executeUpdate();

            var rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }
        return 0;
    }


    public boolean isTableEmpty() {
        try (PreparedStatement pstmt = connection.prepareStatement(SELECT_IS_TABLE_EMPTY);
             ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1) == 0;
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }
        return false;
    }
}
