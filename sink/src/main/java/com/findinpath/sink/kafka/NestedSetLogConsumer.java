package com.findinpath.sink.kafka;

import com.findinpath.sink.model.NestedSetNode;
import com.findinpath.sink.service.NestedSetLogService;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class NestedSetLogConsumer implements Runnable{

    public static final String CONSUMER_GROUP_ID = "nested-set-node.sink";
    private static final long POLL_INTERVAL_MS = 100L;
    private static final Logger LOGGER = LoggerFactory.getLogger(NestedSetLogConsumer.class);


    private final NestedSetLogService nestedSetLogService;
    private final KafkaConsumer<String, GenericRecord> consumer;
    private final String topicName;

    private volatile boolean stopping;   // indicates whether the Worker has asked the task to stop


    public NestedSetLogConsumer(String kafkaBootstrapServers,
                                String schemaRegistryUrl,
                                String topicName,
                                NestedSetLogService nestedSetLogService) {
        this.consumer = createNestedSetLogKafkaConsumer(kafkaBootstrapServers, schemaRegistryUrl, CONSUMER_GROUP_ID);
        this.topicName = topicName;
        this.nestedSetLogService = nestedSetLogService;
    }

    protected boolean isStopping() {
        return stopping;
    }

    @Override
    public void run() {
        try {
            initializeAndStart();
        } finally {
            doClose();
        }
    }

    private void doClose() {
        try {
            close();
        } catch (Throwable t) {
            LOGGER.error("{} Task threw an uncaught and unrecoverable exception during shutdown", this, t);
            throw t;
        }
    }

    private void close() {
        consumer.close();
    }

    public void stop() {
        synchronized (this) {
            stopping = true;
        }
    }

    private void initializeAndStart() {
        consumer.subscribe(Collections.singletonList(topicName));
        iteration();
    }

    private void iteration() {
        while (!isStopping()) {
            final ConsumerRecords<String, GenericRecord> consumerRecords = consumer
                    .poll(Duration.ofMillis(POLL_INTERVAL_MS));
            if (!consumerRecords.isEmpty()) {

                var nestedSetNodeUpdates = StreamSupport
                        .stream(consumerRecords.spliterator(), false)
                        .map(this::convertToNestedSetLog)
                        .collect(Collectors.toList());
                nestedSetLogService.saveAll(nestedSetNodeUpdates);
            }
        }
    }

    private NestedSetNode convertToNestedSetLog(ConsumerRecord<String, GenericRecord> record) {
        var nestedSetNode = new NestedSetNode();
        var recordValue = record.value();
        nestedSetNode.setId((Long)recordValue.get("id"));
        nestedSetNode.setLabel(recordValue.get("label").toString());
        nestedSetNode.setLeft((Integer)recordValue.get("lft"));
        nestedSetNode.setRight((Integer)recordValue.get("rgt"));
        nestedSetNode.setActive((Boolean)recordValue.get("active"));
        nestedSetNode.setCreated(Instant.ofEpochMilli((Long)recordValue.get("created")));
        nestedSetNode.setUpdated(Instant.ofEpochMilli((Long)recordValue.get("updated")));
        return nestedSetNode;
    }




    private static KafkaConsumer<String, GenericRecord> createNestedSetLogKafkaConsumer(
            String bootstrapServers,
            String schemaRegistryUrl,
            String consumerGroupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
                TopicNameStrategy.class.getName());
        return new KafkaConsumer<>(props);
    }
}
