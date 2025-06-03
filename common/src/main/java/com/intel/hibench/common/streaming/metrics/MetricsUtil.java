package com.intel.hibench.common.streaming.metrics;

import com.intel.hibench.common.streaming.Platform;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import scala.Int;

import java.util.Collections;
import java.util.Properties;

public class MetricsUtil {
    public static final String TOPIC_CONF_FILE_NAME = "metrics_topic.conf";

    public static String getTopic(Platform platform, String sourceTopic, Integer producerNum, Long recordPerInterval, Integer intervalSpan ) {
        String topic = String.format("%s_%s_%s_%s_%s_%s", platform, sourceTopic, producerNum, recordPerInterval, intervalSpan, System.currentTimeMillis());
        System.out.println("metrics is being written to kafka topic " + topic);
        return topic;
    }

    public static void createTopic(String zkConnect, String bootstrapServer, String topic, int partitionNum) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("zookeeper.connect", zkConnect);

        try (AdminClient adminClient = AdminClient.create(props)) {
                NewTopic newTopic = new NewTopic(topic, partitionNum, (short) 1);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic created successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
