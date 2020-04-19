package com.findinpath.sink.service;

import com.findinpath.sink.Utils;
import com.findinpath.sink.jdbc.ConnectionProvider;
import com.findinpath.sink.jdbc.NestedSetNodeLogRepository;
import com.findinpath.sink.kafka.NestedSetLogUpdatedEvent;
import com.findinpath.sink.model.NestedSetNode;
import com.google.common.eventbus.EventBus;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class NestedSetLogService {

    private final EventBus eventBus;
    private final ConnectionProvider connectionProvider;

    public NestedSetLogService(EventBus eventBus, ConnectionProvider connectionProvider) {
        this.eventBus = eventBus;
        this.connectionProvider = connectionProvider;
    }

    public void saveAll(List<NestedSetNode> nestedSetNodeList) {
        try (Connection connection = connectionProvider.getConnection()) {
            connection.setAutoCommit(false);
            var nestedSetLogRepository = new NestedSetNodeLogRepository(connection);
            nestedSetLogRepository.saveAll(nestedSetNodeList);
            connection.commit();
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }

        eventBus.post(new NestedSetLogUpdatedEvent());

    }
}
