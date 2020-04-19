package com.findinpath;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.findinpath.kafkaconnect.model.ConnectorConfiguration;
import com.findinpath.testcontainers.KafkaConnectContainer;
import com.findinpath.testcontainers.KafkaContainer;
import com.findinpath.testcontainers.SchemaRegistryContainer;
import com.findinpath.testcontainers.ZookeeperContainer;
import io.restassured.http.ContentType;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static java.lang.String.format;

/**
 * Abstract class used for testing the accuracy of syncing
 * a nested set model via kafka-connect-jdbc.
 * <p>
 * This class groups the complexity related to setting up before all the tests:
 *
 * <ul>
 *     <li>Apache Kafka ecosystem test containers</li>
 *     <li>PostgreSQL source database test container</li>
 *     <li>PostgreSQL sink database test container</li>
 * </ul>
 * <p>
 * and also setting up before each test:
 * <ul>
 *     <li>truncate the content of the source and sink databases</li>
 *     <li>kafka-connect-jdbc connector for the source <code>nested_set_node</code> table</li>
 * </ul>
 * and tearing down after each test:
 * <ul>
 *     <li>kafka-connect-jdbc connector for the source <code>nested_set_node</code> table</li>
 * </ul>
 */
public abstract class AbstractNestedSetSyncTest {

    private static final String KAFKA_CONNECT_CONNECTOR_NAME = "findinpath";
    private static final String NESTED_SET_NODE_SOURCE_TABLE_NAME = "nested_set_node";

    private static final String POSTGRES_DB_DRIVER_CLASS_NAME = "org.postgresql.Driver";

    protected static final String POSTGRES_SOURCE_DB_NAME = "source";
    protected static final String POSTGRES_SOURCE_NETWORK_ALIAS = "source";
    protected static final String POSTGRES_SOURCE_DB_USERNAME = "sa";
    protected static final String POSTGRES_SOURCE_DB_PASSWORD = "p@ssw0rd!source";
    protected static final int POSTGRES_SOURCE_INTERNAL_PORT = 5432;

    private static final String TRUNCATE_SOURCE_NESTED_SET_NODE_SQL =
            "TRUNCATE nested_set_node";

    protected static final String POSTGRES_SINK_DB_NAME = "sink";
    protected static final String POSTGRES_SINK_NETWORK_ALIAS = "sink";
    protected static final String POSTGRES_SINK_DB_USERNAME = "sa";
    protected static final String POSTGRES_SINK_DB_PASSWORD = "p@ssw0rd!sink";

    private static final String TRUNCATE_SINK_NESTED_SET_NODE_SQL =
            "TRUNCATE nested_set_node";

    private static final String TRUNCATE_SINK_NESTED_SET_NODE_LOG_SQL =
            "TRUNCATE nested_set_node_log";

    private static final String TRUNCATE_SINK_LOG_OFFSET_SQL =
            "TRUNCATE log_offset";

    /**
     * Postgres JDBC connection URL to be used within the docker environment.
     */
    private static final String POSTGRES_SOURCE_INTERNAL_CONNECTION_URL = format("jdbc:postgresql://%s:%d/%s?loggerLevel=OFF",
            POSTGRES_SOURCE_NETWORK_ALIAS,
            POSTGRES_SOURCE_INTERNAL_PORT,
            POSTGRES_SOURCE_DB_NAME);

    private static Network network;

    protected static ZookeeperContainer zookeeperContainer;
    protected static KafkaContainer kafkaContainer;
    protected static SchemaRegistryContainer schemaRegistryContainer;
    protected static KafkaConnectContainer kafkaConnectContainer;

    protected static PostgreSQLContainer sourcePostgreSQLContainer;
    protected static PostgreSQLContainer sinkPostgreSQLContainer;

    private static final ObjectMapper mapper = new ObjectMapper();

    protected com.findinpath.source.jdbc.ConnectionProvider sourceConnectionProvider;

    protected com.findinpath.sink.jdbc.ConnectionProvider sinkConnectionProvider;

    private String testUuid;


    /**
     * Bootstrap the docker instances needed for interacting with :
     * <ul>
     *     <li>Confluent Kafka ecosystem</li>
     *     <li>PostgreSQL source</li>
     *     <li>PostgreSQL sink</li>
     * </ul>
     * <p>
     * and subsequently register the kafka-connect-jdbc connector on the
     * source nested_set_node database table.
     */
    @BeforeAll
    public static void dockerSetup() throws Exception {
        network = Network.newNetwork();

        // Confluent setup
        zookeeperContainer = new ZookeeperContainer()
                .withNetwork(network);
        kafkaContainer = new KafkaContainer(zookeeperContainer.getInternalUrl())
                .withNetwork(network);
        schemaRegistryContainer = new SchemaRegistryContainer(zookeeperContainer.getInternalUrl())
                .withNetwork(network);
        kafkaConnectContainer = new KafkaConnectContainer(kafkaContainer.getInternalBootstrapServersUrl())
                .withNetwork(network)
                .withPlugins("plugins/kafka-connect-jdbc/postgresql-42.2.12.jar")
                .withKeyConverter("org.apache.kafka.connect.storage.StringConverter")
                .withValueConverter("io.confluent.connect.avro.AvroConverter")
                .withSchemaRegistryUrl(schemaRegistryContainer.getInternalUrl());

        // Postgres setup
        sourcePostgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
                .withNetwork(network)
                .withNetworkAliases(POSTGRES_SOURCE_NETWORK_ALIAS)
                .withInitScript("source/postgres/init_postgres.sql")
                .withDatabaseName(POSTGRES_SOURCE_DB_NAME)
                .withUsername(POSTGRES_SOURCE_DB_USERNAME)
                .withPassword(POSTGRES_SOURCE_DB_PASSWORD);


        sinkPostgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
                .withNetwork(network)
                .withNetworkAliases(POSTGRES_SINK_NETWORK_ALIAS)
                .withInitScript("sink/postgres/init_postgres.sql")
                .withDatabaseName(POSTGRES_SINK_DB_NAME)
                .withUsername(POSTGRES_SINK_DB_USERNAME)
                .withPassword(POSTGRES_SINK_DB_PASSWORD);

        Startables
                .deepStart(Stream.of(zookeeperContainer,
                        kafkaContainer,
                        schemaRegistryContainer,
                        kafkaConnectContainer,
                        sourcePostgreSQLContainer,
                        sinkPostgreSQLContainer)
                )
                .join();

        verifyKafkaConnectHealth();
    }

    protected void setup() {
        sourceConnectionProvider = new com.findinpath.source.jdbc.ConnectionProvider(POSTGRES_DB_DRIVER_CLASS_NAME,
                sourcePostgreSQLContainer.getJdbcUrl(),
                POSTGRES_SOURCE_DB_USERNAME,
                POSTGRES_SOURCE_DB_PASSWORD
        );
        truncateSourceTables();


        sinkConnectionProvider = new com.findinpath.sink.jdbc.ConnectionProvider(POSTGRES_DB_DRIVER_CLASS_NAME,
                sinkPostgreSQLContainer.getJdbcUrl(),
                POSTGRES_SINK_DB_USERNAME,
                POSTGRES_SINK_DB_PASSWORD
        );
        truncateSinkTables();

        testUuid = UUID.randomUUID().toString();

        setupKakfaConnectJdbcNestedSetNodeSourceTableConnector(testUuid);
    }


    protected void tearDown() {
        deleteKakfaConnectJdbcNestedSetNodeSourceTableConnector(testUuid);
    }

    protected String getKafkaConnectOutputTopic() {
        return getKafkaConnectOutputTopicPrefix(testUuid) + NESTED_SET_NODE_SOURCE_TABLE_NAME;
    }

    private static void setupKakfaConnectJdbcNestedSetNodeSourceTableConnector(String testUuid) {
        var nestedSetNodeSourceConnectorConfig = createConnectorConfig(
                getKafkaConnectorName(testUuid),
                NESTED_SET_NODE_SOURCE_TABLE_NAME,
                POSTGRES_SOURCE_INTERNAL_CONNECTION_URL,
                POSTGRES_SOURCE_DB_USERNAME,
                POSTGRES_SOURCE_DB_PASSWORD,
                getKafkaConnectOutputTopicPrefix(testUuid));
        registerNestedSetNodeSourceTableConnector(nestedSetNodeSourceConnectorConfig);
    }


    private static void deleteKakfaConnectJdbcNestedSetNodeSourceTableConnector(String testUuid) {
        given()
                .log().uri()
                .contentType(ContentType.JSON)
                .accept(ContentType.JSON)
                .when()
                .delete(kafkaConnectContainer.getUrl() + "/connectors/" + getKafkaConnectorName(testUuid))
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }


    private static String getKafkaConnectorName(String suffix) {
        return KAFKA_CONNECT_CONNECTOR_NAME + "-" + suffix;
    }

    private static String getKafkaConnectOutputTopicPrefix(String extraPrefix) {
        return KAFKA_CONNECT_CONNECTOR_NAME + "." + extraPrefix + ".";
    }

    private static String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static ConnectorConfiguration createConnectorConfig(String connectorName,
                                                                String tableName,
                                                                String connectionUrl,
                                                                String connectionUser,
                                                                String connectionPassword,
                                                                String topicPrefix) {

        var config = new HashMap<String, String>();
        config.put("name", connectorName);
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        config.put("tasks.max", "1");
        config.put("connection.url", connectionUrl);
        config.put("connection.user", connectionUser);
        config.put("connection.password", connectionPassword);
        config.put("table.whitelist", tableName);
        config.put("mode", "timestamp+incrementing");
        config.put("timestamp.column.name", "updated");
        config.put("validate.non.null", "false");
        config.put("incrementing.column.name", "id");
        config.put("topic.prefix", topicPrefix);

        return new ConnectorConfiguration(connectorName, config);
    }

    private static void registerNestedSetNodeSourceTableConnector(ConnectorConfiguration connectorConfiguration) {
        given()
                .log().all()
                .contentType(ContentType.JSON)
                .accept(ContentType.JSON)
                .body(toJson(connectorConfiguration))
                .when()
                .post(kafkaConnectContainer.getUrl() + "/connectors")
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_CREATED);
    }

    /**
     * Simple HTTP check to see that the kafka-connect server is available.
     */
    private static void verifyKafkaConnectHealth() {
        given()
                .log().headers()
                .contentType(ContentType.JSON)
                .when()
                .get(kafkaConnectContainer.getUrl())
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_OK);
    }

    private void truncateSourceTables() {
        try (Connection connection = sourceConnectionProvider.getConnection();
             PreparedStatement pstmt = connection.prepareStatement(TRUNCATE_SOURCE_NESTED_SET_NODE_SQL)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            com.findinpath.source.Utils.sneakyThrow(e);
        }
    }

    private void truncateSinkTables() {
        try (Connection connection = sinkConnectionProvider.getConnection();
             PreparedStatement pstmtNestedSetNode = connection.prepareStatement(TRUNCATE_SINK_NESTED_SET_NODE_SQL);
             PreparedStatement pstmtNestedSetNodeLog = connection.prepareStatement(TRUNCATE_SINK_NESTED_SET_NODE_LOG_SQL);
             PreparedStatement pstmtNestedSetNodeLogOffset = connection.prepareStatement(TRUNCATE_SINK_LOG_OFFSET_SQL);
        ) {
            pstmtNestedSetNode.executeUpdate();
            pstmtNestedSetNodeLog.executeUpdate();
            pstmtNestedSetNodeLogOffset.executeUpdate();
        } catch (SQLException e) {
            com.findinpath.sink.Utils.sneakyThrow(e);
        }
    }
}
