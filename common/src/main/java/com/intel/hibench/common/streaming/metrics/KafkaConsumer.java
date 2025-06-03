package com.intel.hibench.common.streaming.metrics;

import kafka.common.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class KafkaConsumer {

    String zookeeper;
    String bootstrapServers;
    String topic;
    Integer partition;

    public static final String CLIENT_ID = "metrics_reader";
    Properties props = new Properties();

    private ConsumerConfig config;
    private Consumer consumer;

    private long nextOffset;
    private Iterator<ConsumerRecord<byte[], byte[]>> iterator;


    public KafkaConsumer(String zookeeper, String bootstrapServers, String topic, Integer partition) {
        this.zookeeper = zookeeper;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.partition = partition;
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", CLIENT_ID);
        config = new ConsumerConfig(props);
        consumer = createConsumer();
        Map<TopicPartition, Long> offsets = consumer.beginningOffsets(Collections.singleton(new TopicPartition(topic, partition)));
        this.nextOffset = offsets.get(offsets.keySet().iterator().next());
        this.iterator = getIterator(nextOffset);
    }

    public List<ConsumerRecord<byte[], byte[]>> next() {
        iterator.next();
    }

    public boolean hasNext() {

    }

}
