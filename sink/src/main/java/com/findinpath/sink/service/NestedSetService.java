package com.findinpath.sink.service;

import com.findinpath.sink.Utils;
import com.findinpath.sink.jdbc.ConnectionProvider;
import com.findinpath.sink.jdbc.NestedSetNodeRepository;
import com.findinpath.sink.model.NestedSetNode;
import com.findinpath.sink.model.NestedSetUpdatedEvent;
import com.findinpath.sink.model.TreeNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

public class NestedSetService {
    private static final Logger LOGGER = LoggerFactory.getLogger(NestedSetService.class);

    private static final String NESTED_SET_KEY = "tree";

    private final ConnectionProvider connectionProvider;
    private final LoadingCache<String, Optional<TreeNode>> treeCache;
    private final LoadingCache<Long, Optional<NestedSetNode>> nestedSetNodeCache;

    public NestedSetService(ConnectionProvider connectionProvider,
                            EventBus eventBus) {
        this.connectionProvider = connectionProvider;
        eventBus.register(this);

        treeCache = CacheBuilder.newBuilder()
                .build(
                        new CacheLoader<>() {
                            @Override
                            public Optional<TreeNode> load(String key) {
                                if (NESTED_SET_KEY.equals(key)) {
                                    return buildTree();
                                }

                                return Optional.empty();
                            }
                        }
                );

        nestedSetNodeCache = CacheBuilder.newBuilder()
                .build(
                        new CacheLoader<>() {
                            @Override
                            public Optional<NestedSetNode> load(Long key) {
                               return getNestedSetNode(key);

                            }
                        }
                );
    }

    public Optional<TreeNode> getTree() {
        return treeCache.getUnchecked(NESTED_SET_KEY);
    }

    public Optional<NestedSetNode> getNestedSetNode(long nodeId){
        try (Connection connection = connectionProvider.getConnection()) {
            var nestedSetNodeRepository = new NestedSetNodeRepository(connection);
            return nestedSetNodeRepository.getNestedSetNode(nodeId);

        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return Optional.empty();
        }
    }

    private Optional<TreeNode> buildTree() {
        LOGGER.info("Building the tree from the persistence");

        try (Connection connection = connectionProvider.getConnection()) {
            var nestedSetNodeRepository = new NestedSetNodeRepository(connection);
            var nestedSetNodes = nestedSetNodeRepository.getNestedSetNodes();
            if (nestedSetNodes.isEmpty()) {
                return Optional.empty();
            }

            var tree = TreeUtils.buildTree(nestedSetNodes);
            if (tree.isEmpty()) {
                LOGGER.error("The nested_set_node table content is corrupt");
            }
            return tree;
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return Optional.empty();
        }
    }

    @Subscribe
    public void updateTree(NestedSetUpdatedEvent event) {
        treeCache.invalidate(NESTED_SET_KEY);
        nestedSetNodeCache.invalidateAll();
    }
}
