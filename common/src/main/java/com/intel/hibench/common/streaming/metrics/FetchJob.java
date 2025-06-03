package com.intel.hibench.common.streaming.metrics;

import com.codahale.metrics.Histogram;

import java.util.concurrent.Callable;

public class FetchJob implements Callable<FetchJobResult> {

    String zkConnect;
    String bootstrapServer;
    String topic;
    Integer partition;
    Histogram histogram;

    public FetchJob(String zkConnect, String bootstrapServer, String topic, Integer partition, Histogram histogram) {
        this.zkConnect = zkConnect;
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.partition = partition;
        this.histogram = histogram;
    }

    @Override
    public FetchJobResult call() throws Exception {
        FetchJobResult result = new FetchJobResult();
        KafkaConsumer consumer = new KafkaConsumer(zkConnect, bootstrapServer, topic, partition);
        while (consumer.hasNext()) {
            // todo...
            String[] times = new String().split(":");
            Long startTime =Long.parseLong(times[0]);
            Long endTime = Long.parseLong(times[1]);
            // correct negative value which might be caused by difference of system time
            histogram.update(Math.max(0, endTime - startTime));
            result.update(startTime, endTime);
        }
        System.out.println("Collected ${result.count} results for partition: " + partition);
        return result;
    }
}
