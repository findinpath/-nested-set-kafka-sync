package com.findinpath.sink.service;

import com.findinpath.sink.Utils;
import com.findinpath.sink.jdbc.ConnectionProvider;
import com.findinpath.sink.kafka.NestedSetLogUpdatedEvent;
import com.findinpath.sink.model.NestedSetNode;
import com.findinpath.sink.model.NestedSetUpdatedEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Testcontainers
public class NestedSetLogServiceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(NestedSetLogServiceTest.class);


    private static final String POSTGRES_DB_NAME = "findinpath";
    private static final String POSTGRES_NETWORK_ALIAS = "postgres";
    private static final String POSTGRES_DB_USERNAME = "sa";
    private static final String POSTGRES_DB_PASSWORD = "p@ssw0rd!";
    private static final String POSTGRES_DB_DRIVER_CLASS_NAME = "org.postgresql.Driver";

    private static final String TRUNCATE_NESTED_SET_NODE_SQL =
            "TRUNCATE nested_set_node";

    private static final String TRUNCATE_NESTED_SET_NODE_LOG_SQL =
            "TRUNCATE nested_set_node_log";

    private static final String TRUNCATE_LOG_OFFSET_SQL =
            "TRUNCATE log_offset";

    private static final ConditionFactory WAIT = await().atMost(5, TimeUnit.SECONDS);

    @Container
    private static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
            .withNetworkAliases(POSTGRES_NETWORK_ALIAS)
            .withInitScript("sink/postgres/init_postgres.sql")
            .withDatabaseName(POSTGRES_DB_NAME)
            .withUsername(POSTGRES_DB_USERNAME)
            .withPassword(POSTGRES_DB_PASSWORD);


    private NestedSetLogService nestedSetLogService;

    private SquashingNestedSetLogUpdateListener squashingNestedSetLogUpdateListener;

    private NestedSetSyncService nestedSetSyncService;

    private NestedSetService nestedSetService;

    private ConnectionProvider connectionProvider;

    private EventBus eventBus;

    private Optional<Instant> lastNestedSetNodeTableUpdate;
    private Optional<Instant> lastNestedSetNodeLogTableUpdate;

    @BeforeEach
    public void beforeEach() {
        connectionProvider = new ConnectionProvider(POSTGRES_DB_DRIVER_CLASS_NAME,
                postgreSQLContainer.getJdbcUrl(),
                POSTGRES_DB_USERNAME,
                POSTGRES_DB_PASSWORD
        );

        eventBus = new EventBus();

        resetLastNestedSetNodeTablesUpdateInstants();
        eventBus.register(this);

        nestedSetService = new NestedSetService(connectionProvider, eventBus);
        nestedSetLogService = new NestedSetLogService(eventBus, connectionProvider);
        nestedSetSyncService = new NestedSetSyncService(connectionProvider, eventBus);
        squashingNestedSetLogUpdateListener = new SquashingNestedSetLogUpdateListener(
                nestedSetLogUpdatedEvent -> nestedSetSyncService.onNestedSetLogUpdate(), eventBus);


        truncateTables();
    }

    @AfterEach
    public void afterEach() {
        squashingNestedSetLogUpdateListener.stop();
    }

    @Test
    public void saveTheRootNodeAccuracy() throws Exception {
        var newNestedSetNode = new NestedSetNode(1, "A", 1, 2, true,
                Instant.now().truncatedTo(ChronoUnit.MILLIS),
                Instant.now().truncatedTo(ChronoUnit.MILLIS));

        nestedSetLogService.saveAll(List.of(newNestedSetNode));

        WAIT.until(() -> lastNestedSetNodeTableUpdate.isPresent());

        var rootNode = nestedSetService.getTree()
                .orElseThrow(() -> new IllegalStateException("nested set hasn't been initialized"));

        assertThat(rootNode.getNestedSetNode(), equalTo(newNestedSetNode));
    }


    @Test
    public void saveTheRootNodeAndChildAccuracy() throws Exception {
        var newRootNode = new NestedSetNode(1, "A", 1, 4, true,
                Instant.now().truncatedTo(ChronoUnit.MILLIS),
                Instant.now().truncatedTo(ChronoUnit.MILLIS));
        var newChildNode = new NestedSetNode(2, "B", 2, 3, true,
                Instant.now().truncatedTo(ChronoUnit.MILLIS),
                Instant.now().truncatedTo(ChronoUnit.MILLIS));

        nestedSetLogService.saveAll(List.of(newRootNode, newChildNode));

        WAIT.until(() -> lastNestedSetNodeTableUpdate.isPresent());

        var rootNode = nestedSetService.getTree()
                .orElseThrow(() -> new IllegalStateException("nested set hasn't been initialized"));
        assertThat(rootNode.getNestedSetNode(), equalTo(newRootNode));
        var childNode = rootNode.getChildren().get(0);
        assertThat(childNode.getNestedSetNode(), equalTo(newChildNode));
    }


    @Test
    public void saveOnTopOfExistingNestedSetAccuracy() throws Exception {
        var rootNode1 = new NestedSetNode(1, "A", 1, 2, true,
                Instant.now().truncatedTo(ChronoUnit.MILLIS),
                Instant.now().truncatedTo(ChronoUnit.MILLIS));

        nestedSetLogService.saveAll(List.of(rootNode1));

        var rootNode2 = new NestedSetNode(1, "A", 1, 4, true,
                rootNode1.getCreated(),
                Instant.now().truncatedTo(ChronoUnit.MILLIS));
        var childNode2 = new NestedSetNode(2, "B", 2, 3, true,
                Instant.now().truncatedTo(ChronoUnit.MILLIS),
                Instant.now().truncatedTo(ChronoUnit.MILLIS));

        resetLastNestedSetNodeTablesUpdateInstants();
        nestedSetLogService.saveAll(List.of(rootNode2, childNode2));
        waitUntilNextNestedSetNodeTableUpdate();

        var retrievedRootNode = nestedSetService.getTree()
                .orElseThrow(() -> new IllegalStateException("nested set hasn't been initialized"));

        assertThat(retrievedRootNode.getNestedSetNode(), equalTo(rootNode2));
        var retrievedChildNode = retrievedRootNode.getChildren().get(0);
        assertThat(retrievedChildNode.getNestedSetNode(), equalTo(childNode2));
    }

    @Test
    public void saveOnTopOfExistingNestedSetSlightlyOutOfOrderAccuracy() throws Exception {
        var rootNode1 = new NestedSetNode(1, "A", 1, 2, true,
                Instant.now().truncatedTo(ChronoUnit.MILLIS),
                Instant.now().truncatedTo(ChronoUnit.MILLIS));

        nestedSetLogService.saveAll(List.of(rootNode1));

        WAIT.until(() -> lastNestedSetNodeTableUpdate.isPresent());

        // the second version of the root node comes initially the log without its corresponding child
        var rootNode2 = new NestedSetNode(1, "A", 1, 4, true,
                rootNode1.getCreated(),
                Instant.now().truncatedTo(ChronoUnit.MILLIS));

        resetLastNestedSetNodeTablesUpdateInstants();
        nestedSetLogService.saveAll(List.of(rootNode2));
        WAIT.until(() -> lastNestedSetNodeLogTableUpdate.isPresent());

        // the nested node should remain at this time unchanged.
        var retrievedRootNode = nestedSetService.getTree()
                .orElseThrow(() -> new IllegalStateException("nested set hasn't been initialized"));

        assertThat(retrievedRootNode.getNestedSetNode(), equalTo(rootNode1));

        var childNode2 = new NestedSetNode(2, "B", 2, 3, true,
                Instant.now().truncatedTo(ChronoUnit.MILLIS),
                Instant.now().truncatedTo(ChronoUnit.MILLIS));

        resetLastNestedSetNodeTablesUpdateInstants();
        nestedSetLogService.saveAll(List.of(childNode2));
        waitUntilNextNestedSetNodeTableUpdate();

        retrievedRootNode = nestedSetService.getTree()
                .orElseThrow(() -> new IllegalStateException("nested set hasn't been initialized"));

        assertThat(retrievedRootNode.getNestedSetNode(), equalTo(rootNode2));
        var retrievedChildNode = retrievedRootNode.getChildren().get(0);
        assertThat(retrievedChildNode.getNestedSetNode(), equalTo(childNode2));
    }

    @Subscribe
    public void onNestedSetUpdatedEvent(NestedSetUpdatedEvent e) {
        var now = Instant.now();
        LOGGER.info("Received notification about new updates in the nested_set_node table at " + now);
        lastNestedSetNodeTableUpdate = Optional.of(now);
    }

    @Subscribe
    public void onNestedSetLogUpdatedEvent(NestedSetLogUpdatedEvent e) {
        var now = Instant.now();
        LOGGER.info("Received notification about new entries in the nested_set_node_log table at " + now);
        lastNestedSetNodeLogTableUpdate = Optional.of(now);

    }

    private void waitUntilNextNestedSetNodeTableUpdate() {
        WAIT.until(() -> lastNestedSetNodeTableUpdate.isPresent()
                && lastNestedSetNodeLogTableUpdate.isPresent()
                && lastNestedSetNodeTableUpdate.get().isAfter(lastNestedSetNodeLogTableUpdate.get()));
    }

    private void resetLastNestedSetNodeTablesUpdateInstants() {
        LOGGER.info("Resetting last nested set node tables instants");
        lastNestedSetNodeTableUpdate = Optional.empty();
        lastNestedSetNodeLogTableUpdate = Optional.empty();
    }

    private void truncateTables() {
        try (Connection connection = connectionProvider.getConnection();
             PreparedStatement pstmtNestedSetNode = connection.prepareStatement(TRUNCATE_NESTED_SET_NODE_SQL);
             PreparedStatement pstmtNestedSetNodeLog = connection.prepareStatement(TRUNCATE_NESTED_SET_NODE_LOG_SQL);
             PreparedStatement pstmtNestedSetNodeLogOffset = connection.prepareStatement(TRUNCATE_LOG_OFFSET_SQL);
        ) {
            pstmtNestedSetNode.executeUpdate();
            pstmtNestedSetNodeLog.executeUpdate();
            pstmtNestedSetNodeLogOffset.executeUpdate();
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }
    }
}
