package com.intel.hibench.common.streaming.metrics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaReporter implements LatencyReporter {

    private static volatile KafkaProducer<String, String> instance;

    public static synchronized KafkaProducer<String, String> getInstance(String bootstrapServers) {
        if (instance == null) {
            synchronized (KafkaReporter.class) {
                if (instance == null) {
                    Properties props = new Properties();
                    props.put("bootstrap.servers", bootstrapServers);
                    instance = new KafkaProducer<>(
                            props,
                            new StringSerializer(),
                            new StringSerializer()
                    );
                }
            }
        }
        return instance;
    }

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaReporter(String topic, String bootstrapServers) {
        this.topic = topic;
        this.producer = KafkaReporter.getInstance(bootstrapServers);
    }

    @Override
    public void report(long startTime, long endTime) {
        producer.send(new ProducerRecord<>(topic, (String) null, startTime + ":" + endTime));
    }
}

