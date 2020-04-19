package com.findinpath;


import com.findinpath.sink.kafka.NestedSetLogConsumer;
import com.findinpath.sink.service.NestedSetLogService;
import com.findinpath.sink.service.NestedSetSyncService;
import com.findinpath.sink.service.SquashingNestedSetLogUpdateListener;
import com.google.common.eventbus.EventBus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * This class contains a series of end-to-end tests verifying
 * that the sync of a nested set model via kafka-connect-jdbc
 * works as expected.
 * <p>
 * The tests involve the following preparation steps:
 * <ul>
 *     <li>bootstrapping testcontainers for:
 *      <ul>
 *             <li>PostgreSQL source and sink database</li>
 *          <li>Apache Kafka ecosystem</li>
 *      </ul>
 *     </li>
 *     <li>registering a kafka-connect-jdbc connector for the source database table containing the nested set model</li>
 * </ul>
 * <p>
 * The end-to-end tests make sure that the contents of the nested set model
 * are synced from source to sink even independently whether the tree suffers small or bigger changes.
 * NOTE that adding a node to the nested set model involves changing the left and right coordinates
 * of a big portion of the nested set.
 *
 * @see {@link AbstractNestedSetSyncTest}
 */
public class DemoNestedSetSyncTest extends AbstractNestedSetSyncTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoNestedSetSyncTest.class);

    private com.findinpath.source.service.NestedSetService sourceNestedSetService;

    private ExecutorService sinkNestedLogConsumerExecutorService;
    private ExecutorService sinkNestedSetLogUpdateExecutorService;
    private com.findinpath.sink.service.NestedSetService sinkNestedSetService;
    private NestedSetLogConsumer sinkNestedSetLogConsumer;

    @BeforeEach
    public void setup() {
        super.setup();

        sourceNestedSetService = new com.findinpath.source.service.NestedSetService(sourceConnectionProvider);
        setupSinkServices();
    }

    private void setupSinkServices() {
        var sinkEventBus = new EventBus();
        sinkNestedSetService = new com.findinpath.sink.service.NestedSetService(sinkConnectionProvider, sinkEventBus);
        var sinkNestedSetLogService = new NestedSetLogService(sinkEventBus, sinkConnectionProvider);
        var sinkNestedSetSyncService = new NestedSetSyncService(sinkConnectionProvider, sinkEventBus);
        sinkNestedSetLogUpdateExecutorService = Executors.newSingleThreadExecutor();
        var sinkNestedSetLogUpdateListener = new SquashingNestedSetLogUpdateListener(
                nestedSetLogUpdatedEvent -> sinkNestedSetSyncService.onNestedSetLogUpdate(),
                sinkNestedSetLogUpdateExecutorService, sinkEventBus);

        sinkNestedSetLogConsumer = new NestedSetLogConsumer(kafkaContainer.getBootstrapServersUrl(),
                schemaRegistryContainer.getUrl(),
                getKafkaConnectOutputTopic(),
                sinkNestedSetLogService);
        sinkNestedLogConsumerExecutorService = Executors.newSingleThreadExecutor();
        sinkNestedLogConsumerExecutorService.execute(sinkNestedSetLogConsumer);
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();

        sinkNestedSetLogConsumer.stop();
        sinkNestedLogConsumerExecutorService.shutdown();
        sinkNestedSetLogUpdateExecutorService.shutdown();
    }

    /**
     * This test ensures the sync accuracy for the following simple tree:
     *
     * <pre>
     * |1| A |6|
     *     ├── |2| B |3|
     *     └── |4| C |5|
     * </pre>
     */
    @Test
    public void simpleTreeDemo() {

        // Create a nested set in the persistence of the source database
        // with the following configuration:
        //  left=1  label=A   right=6
        //            left = 2  label=B  right = 3
        //            left = 4  label=C  right = 5

        var aNodeLabel = "A";
        var aNodeId = sourceNestedSetService.insertRootNode(aNodeLabel);
        var bNodeLabel = "B";
        var bNodeId = sourceNestedSetService.insertNode(bNodeLabel, aNodeId);
        var cNodeLabel = "C";
        var cNodeId = sourceNestedSetService.insertNode(cNodeLabel, aNodeId);

        awaitForTheSyncOfTheNode(cNodeId);
        logSinkTreeContent();
    }


    /**
     * Accuracy test for the sync of the contents of the following nested set model:
     *
     * <pre>
     * |1| Food |18|
     *     ├── |2| Fruit |11|
     *     │       ├── |3| Red |6|
     *     │       │       └── |4| Cherry |5|
     *     │       └── |7| Yellow |10|
     *     │               └── |8| Banana |9|
     *     └── |12| Meat |17|
     *              ├── |13| Beef |14|
     *              └── |15| Pork |16|
     * </pre>
     */
    @Test
    public void foodTreeSyncDemo() {
        var foodNodeId = sourceNestedSetService.insertRootNode("Food");
        var fruitNodeId = sourceNestedSetService.insertNode("Fruit", foodNodeId);
        var redFruitNodeId = sourceNestedSetService.insertNode("Red", fruitNodeId);
        sourceNestedSetService.insertNode("Cherry", redFruitNodeId);
        var yellowFruitNodeId = sourceNestedSetService.insertNode("Yellow", fruitNodeId);
        sourceNestedSetService.insertNode("Banana", yellowFruitNodeId);
        var meatNodeId = sourceNestedSetService.insertNode("Meat", foodNodeId);
        sourceNestedSetService.insertNode("Beef", meatNodeId);
        sourceNestedSetService.insertNode("Pork", meatNodeId);

        awaitForTheSyncOfTheNode(foodNodeId);
        logSinkTreeContent();
    }


    /**
     * This test makes sure that the syncing between source and sink
     * works when successively over the time new nodes are added to the nested
     * set model.
     * <p>
     * At the end of the test the synced nested set model should look like:
     *
     * <pre>
     *     |1| Clothing |14|
     *     ├── |2| Men's |5|
     *     │       └── |3| Suits |4|
     *     └── |6| Women's |13|
     *             ├── |7| Dresses |8|
     *             ├── |9| Skirts |10|
     *             └── |11| Blouses |12|
     * </pre>
     */
    @Test
    public void syncingSuccessiveChangesToTheTreeDemo() {

        // Create initially a nested set with only the root node
        //  left=1  label=Clothing   right=2

        var clothingNodeId = sourceNestedSetService.insertRootNode("Clothing");
        awaitForTheSyncOfTheNode(clothingNodeId);
        logSinkTreeContent();

        // Add now Men's and Women's children and wait for the syncing
        var mensNodeId = sourceNestedSetService.insertNode("Men's", clothingNodeId);
        var womensNodeId = sourceNestedSetService.insertNode("Women's", clothingNodeId);

        awaitForTheSyncOfTheNode(womensNodeId);
        logSinkTreeContent();

        // Add new children categories
        sourceNestedSetService.insertNode("Suits", mensNodeId);
        sourceNestedSetService.insertNode("Dresses", womensNodeId);
        sourceNestedSetService.insertNode("Skirts", womensNodeId);
        sourceNestedSetService.insertNode("Blouses", womensNodeId);

        awaitForTheSyncOfTheNode(womensNodeId);
        logSinkTreeContent();
    }

    private void logSinkTreeContent() {
        var sinkNestedSetRootNode = sinkNestedSetService.getTree()
                .orElseThrow(() -> new IllegalStateException("Sink tree hasn't been initialized"));
        LOGGER.info("Sink nested set node current configuration: \n" + sinkNestedSetRootNode);
    }

    private void awaitForTheSyncOfTheNode(long nodeId) {
        var sourceNestedSetNode = sourceNestedSetService.getNestedSetNode(nodeId)
                .orElseThrow(() -> new IllegalStateException("Node with ID " + nodeId + " was not found"));

        // and now  verify that the syncing via kafka-connect-jdbc
        // between source and sink database works
        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> {
                    var sinkNestedSetNode = sinkNestedSetService.getNestedSetNode(nodeId);
                    if (sinkNestedSetNode.isPresent()) {
                        return assertEquality(sourceNestedSetNode, sinkNestedSetNode.get());
                    }
                    return false;
                });
    }

    private static boolean assertEquality(com.findinpath.source.model.NestedSetNode sourceNestedSetNode,
                                          com.findinpath.sink.model.NestedSetNode sinkNestedSetNode) {
        return Objects.equals(sourceNestedSetNode.getId(), sinkNestedSetNode.getId())
                && Objects.equals(sourceNestedSetNode.getLabel(), sinkNestedSetNode.getLabel())
                && Objects.equals(sourceNestedSetNode.getLeft(), sinkNestedSetNode.getLeft())
                && Objects.equals(sourceNestedSetNode.getRight(), sinkNestedSetNode.getRight())
                && Objects.equals(sourceNestedSetNode.getCreated(), sinkNestedSetNode.getCreated())
                && Objects.equals(sourceNestedSetNode.getUpdated(), sinkNestedSetNode.getUpdated());
    }
}
