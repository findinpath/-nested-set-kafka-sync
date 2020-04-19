package com.findinpath.sink.service;


import com.findinpath.sink.Utils;
import com.findinpath.sink.jdbc.ConnectionProvider;
import com.findinpath.sink.jdbc.LogOffsetRepository;
import com.findinpath.sink.jdbc.NestedSetNodeLogRepository;
import com.findinpath.sink.jdbc.NestedSetNodeRepository;
import com.findinpath.sink.model.NestedSetNode;
import com.findinpath.sink.model.NestedSetNodeLog;
import com.findinpath.sink.model.NestedSetUpdatedEvent;
import com.google.common.base.Functions;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class NestedSetSyncService {

    private static final Logger LOGGER = LoggerFactory.getLogger(NestedSetSyncService.class);

    private static final String NESTED_SET_NODE_LOG_TABLE = "nested_set_node_log";

    private final EventBus eventBus;
    private final ConnectionProvider connectionProvider;

    public NestedSetSyncService(ConnectionProvider connectionProvider,
                                EventBus eventBus) {
        this.connectionProvider = connectionProvider;
        this.eventBus = eventBus;
    }

    public void onNestedSetLogUpdate() {
        LOGGER.info("Received notification about new updates on the nested_set_node_log table");
        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);

            var isNestedTreeNodeTableUpdated = false;
            try {
                final LogOffsetRepository logOffsetRepository = new LogOffsetRepository(connection);
                final NestedSetNodeLogRepository nestedSetNodeLogRepository = new NestedSetNodeLogRepository(connection);
                final NestedSetNodeRepository nestedSetNodeRepository = new NestedSetNodeRepository(connection);

                var nestedSetLogUpdates = nestedSetNodeLogRepository.getNestedSetLogUpdates();
                if (!nestedSetLogUpdates.isEmpty()) {
                    BinaryOperator<NestedSetNodeLog> takeNestedSetNodeLogWithTheMaxId = (nestedSetNodeLog1, nestedSetNodeLog2) ->
                            nestedSetNodeLog1.getId() > nestedSetNodeLog2.getId() ? nestedSetNodeLog1 : nestedSetNodeLog2;

                    var deduplicatedNestedSetLogUpdates = nestedSetLogUpdates
                            .stream()
                            .collect(Collectors.groupingBy(nestedSetNodeLog -> nestedSetNodeLog.getNestedSetNode().getId()))
                            .values()
                            .stream()
                            .map(nestedSetNodeLogsWithTheSameNodeId -> nestedSetNodeLogsWithTheSameNodeId.stream().reduce(takeNestedSetNodeLogWithTheMaxId))
                            .map(Optional::get)
                            .sorted(Comparator.comparing(NestedSetNodeLog::getId))
                            .collect(Collectors.toList());


                    var latestNestedSetLogId = deduplicatedNestedSetLogUpdates.get(deduplicatedNestedSetLogUpdates.size() - 1).getId();

                    var nestedSetNodes = nestedSetNodeRepository.getNestedSetNodes();
                    var id2NestedSetNodeMap = nestedSetNodes.stream()
                            .collect(Collectors.toMap(NestedSetNode::getId, Functions.identity()));

                    Predicate<NestedSetNode> isNestedSetNodeAlreadyPersisted = (NestedSetNode nestedSetNode) ->
                            id2NestedSetNodeMap.containsKey(nestedSetNode.getId());
                    var partitions = deduplicatedNestedSetLogUpdates
                            .stream()
                            .map(NestedSetNodeLog::getNestedSetNode)
                            .collect(Collectors.partitioningBy(isNestedSetNodeAlreadyPersisted));
                    var newNestedSetNodesSortedByLogId = partitions.get(false);
                    var updatedNestedSetNodesSortedByLogId = partitions.get(true);

                    var nestedSetNodesUpdates = deduplicatedNestedSetLogUpdates
                            .stream()
                            .map(NestedSetNodeLog::getNestedSetNode)
                            .collect(Collectors.toList());
                    var updatedTree = TreeUtils.applyUpdates(nestedSetNodes, nestedSetNodesUpdates);
                    if (updatedTree.isPresent()) {
                        logOffsetRepository.saveNestedSetLogOffset(NESTED_SET_NODE_LOG_TABLE, latestNestedSetLogId);
                        nestedSetNodeRepository.insertAll(newNestedSetNodesSortedByLogId);
                        nestedSetNodeRepository.updateAll(updatedNestedSetNodesSortedByLogId);

                        isNestedTreeNodeTableUpdated = true;
                    }
                }
                connection.commit();
                if (isNestedTreeNodeTableUpdated) {
                    eventBus.post(new NestedSetUpdatedEvent());
                }
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }
    }
}
