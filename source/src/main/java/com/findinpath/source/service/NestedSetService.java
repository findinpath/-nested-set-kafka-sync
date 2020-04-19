package com.findinpath.source.service;

import com.findinpath.source.Utils;
import com.findinpath.source.jdbc.ConnectionProvider;
import com.findinpath.source.jdbc.NestedSetNodeRepository;
import com.findinpath.source.model.NestedSetNode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class NestedSetService {

    private final ConnectionProvider connectionProvider;

    public NestedSetService(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public List<NestedSetNode> getNestedSetNodes() {
        try (Connection connection = connectionProvider.getConnection()) {
            var nestedSetNodeRepository = new NestedSetNodeRepository(connection);

            return nestedSetNodeRepository.getNestedSetNodes();
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return null;
        }
    }

    public Optional<NestedSetNode> getNestedSetNode(long id) {
        try (Connection connection = connectionProvider.getConnection()) {
            var nestedSetNodeRepository = new NestedSetNodeRepository(connection);

            return nestedSetNodeRepository.getNestedSetNode(id);
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return null;
        }
    }

    public long insertNode(String data, long parentId) {
        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);
            try {
                var nestedSetNodeRepository = new NestedSetNodeRepository(connection);
                var parentNode = nestedSetNodeRepository.getNestedSetNode(parentId)
                        .orElseThrow(() -> new IllegalArgumentException("Invalid parent id " + parentId));

                nestedSetNodeRepository.makeSpaceForNewNode(parentNode.getRight());

                return nestedSetNodeRepository.insertNode(data, parentNode.getRight(), parentNode.getRight() + 1);
            } finally {
                connection.commit();
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return 0;
        }
    }

    public long insertRootNode(String data) {
        try (Connection connection = connectionProvider.getConnection()) {
            try {
                connection.setAutoCommit(false);
                var nestedSetNodeRepository = new NestedSetNodeRepository(connection);

                var isTableEmpty = nestedSetNodeRepository.isTableEmpty();
                if (!isTableEmpty) {
                    throw new IllegalStateException("The nested_set table already contains data");
                }

                return nestedSetNodeRepository.insertNode(data, 1, 2);
            } finally {
                connection.commit();
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return 0;
        }
    }
}
