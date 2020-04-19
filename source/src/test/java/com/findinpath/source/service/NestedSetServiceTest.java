package com.findinpath.source.service;

import com.findinpath.source.TreeUtils;
import com.findinpath.source.Utils;
import com.findinpath.source.jdbc.ConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Testcontainers
public class NestedSetServiceTest {

    private static final String POSTGRES_DB_NAME = "findinpath";
    private static final String POSTGRES_NETWORK_ALIAS = "postgres";
    private static final String POSTGRES_DB_USERNAME = "sa";
    private static final String POSTGRES_DB_PASSWORD = "p@ssw0rd!";
    private static final String POSTGRES_DB_DRIVER_CLASS_NAME = "org.postgresql.Driver";

    private static final String TRUNCATE_NESTED_SET_NODE_SQL =
            "TRUNCATE nested_set_node";

    @Container
    private static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
            .withNetworkAliases(POSTGRES_NETWORK_ALIAS)
            .withInitScript("source/postgres/init_postgres.sql")
            .withDatabaseName(POSTGRES_DB_NAME)
            .withUsername(POSTGRES_DB_USERNAME)
            .withPassword(POSTGRES_DB_PASSWORD);

    private ConnectionProvider connectionProvider;

    private NestedSetService nestedSetService;

    @BeforeEach
    public void beforeEach() {
        connectionProvider = new ConnectionProvider(POSTGRES_DB_DRIVER_CLASS_NAME,
                postgreSQLContainer.getJdbcUrl(),
                POSTGRES_DB_USERNAME,
                POSTGRES_DB_PASSWORD
        );

        nestedSetService = new NestedSetService(connectionProvider);

        truncateNestedSetNodeTable();
    }

    @Test
    public void verifyAddingRootNode() {
        var rootNodeData = "A";
        var rootNodeId = nestedSetService.insertRootNode(rootNodeData);
        var rootNode = nestedSetService.getNestedSetNode(rootNodeId)
                .orElseThrow(() -> new IllegalStateException("The root node with ID "+ rootNodeId + " should exist in the DB"));

        assertThat(rootNode.getId(), equalTo(rootNodeId));
        assertThat(rootNode.getLabel(), equalTo(rootNodeData));
        assertThat(rootNode.getLeft(), equalTo(1));
        assertThat(rootNode.getRight(), equalTo(2));

    }


    @Test
    public void verifyAddingRootNodeAndFirstLevelChildren() {
        var rootNodeData = "A";
        var rootNodeId = nestedSetService.insertRootNode(rootNodeData);

        var childBData = "B";
        var childBNodeId = nestedSetService.insertNode("B", rootNodeId);

        var childCData = "C";
        var childCNodeId = nestedSetService.insertNode("C", rootNodeId);

        var nestedSetNodes = nestedSetService.getNestedSetNodes();


        var rootNode = TreeUtils.buildTree(nestedSetNodes)
                .orElseThrow(() -> new IllegalStateException("The nested_set_node "));
        assertThat(rootNode.getNestedSetNode().getLabel(), equalTo(rootNodeData));
        var children = rootNode.getChildren();
        assertThat(children.get(0).getNestedSetNode().getLabel(), equalTo(childBData));
        assertThat(children.get(1).getNestedSetNode().getLabel(), equalTo(childCData));
    }


    @Test
    public void verifyAddingRootNodeAndTwoLevelChildren() {
        var rootNodeData = "A";
        var rootNodeId = nestedSetService.insertRootNode(rootNodeData);

        var childBData = "B";
        var childBNodeId = nestedSetService.insertNode("B", rootNodeId);

        var childCData = "C";
        var childCNodeId = nestedSetService.insertNode("C", childBNodeId);

        var nestedSetNodes = nestedSetService.getNestedSetNodes();


        var rootNode = TreeUtils.buildTree(nestedSetNodes)
                .orElseThrow(() -> new IllegalStateException("The nested_set_node "));
        assertThat(rootNode.getNestedSetNode().getLabel(), equalTo(rootNodeData));
        var childrenA = rootNode.getChildren();
        assertThat(childrenA.get(0).getNestedSetNode().getLabel(), equalTo(childBData));
        var childrenB = childrenA.get(0).getChildren();
        assertThat(childrenB.get(0).getNestedSetNode().getLabel(), equalTo(childCData));

    }

    private void truncateNestedSetNodeTable() {
        try (Connection connection = connectionProvider.getConnection();
             PreparedStatement pstmt = connection.prepareStatement(TRUNCATE_NESTED_SET_NODE_SQL)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }
    }
}
