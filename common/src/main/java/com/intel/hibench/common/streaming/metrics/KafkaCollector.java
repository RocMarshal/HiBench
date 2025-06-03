package com.intel.hibench.common.streaming.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;
import com.codahale.metrics.Snapshot;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaCollector implements LatencyCollector {
    private final Histogram histogram;
    private final ExecutorService threadPool;
    private final List<Future<FetchJobResult>> fetchResults;
    private final String zkConnect;
    private final String bootstrapServers;
    private final String metricsTopic;
    private final String outputDir;
    private final int sampleNumber;
    private final int desiredThreadNum;

    public KafkaCollector(String zkConnect, String bootstrapServers, String metricsTopic,
                          String outputDir, int sampleNumber, int desiredThreadNum) {
        this.zkConnect = zkConnect;
        this.bootstrapServers = bootstrapServers;
        this.metricsTopic = metricsTopic;
        this.outputDir = outputDir;
        this.sampleNumber = sampleNumber;
        this.desiredThreadNum = desiredThreadNum;
        this.histogram = new Histogram(new UniformReservoir(sampleNumber));
        this.threadPool = Executors.newFixedThreadPool(desiredThreadNum);
        this.fetchResults = new ArrayList<>();
    }

    @Override
    public void start() throws Exception {
        List<Integer> partitions = getPartitions();

        System.out.println("Starting MetricsReader for kafka topic: " + metricsTopic);

        for (Integer partition : partitions) {
            FetchJob job = new FetchJob(zkConnect, bootstrapServers, metricsTopic, partition, histogram);
            Future<FetchJobResult> fetchFuture = threadPool.submit(job);
            fetchResults.add(fetchFuture);
        }

        threadPool.shutdown();
        threadPool.awaitTermination(30, TimeUnit.MINUTES);

        FetchJobResult finalResult = fetchResults.stream()
                .map(f -> {
                    try {
                        return f.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .reduce((a, b) -> {
                    long minTime = Math.min(a.getMinTime(), b.getMinTime());
                    long maxTime = Math.max(a.getMaxTime(), b.getMaxTime());
                    long count = a.getCount() + b.getCount();
                    return new FetchJobResult(minTime, maxTime, count);
                })
                .orElseThrow(() -> new RuntimeException("No fetch results available"));

        report(finalResult.getMinTime(), finalResult.getMaxTime(), finalResult.getCount());
    }

    private List<Integer> getPartitions() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("zookeeper.connect", zkConnect);

        try (AdminClient adminClient = AdminClient.create(props)) {
            Map<String, KafkaFuture<TopicDescription>> topicDescriptions =
                    adminClient.describeTopics(Collections.singleton(metricsTopic)).topicNameValues();
            return topicDescriptions.get(metricsTopic).get().partitions().stream().map(TopicPartitionInfo::partition).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    private void report(long minTime, long maxTime, long count) throws IOException {
        File outputFile = new File(outputDir, metricsTopic + ".csv");
        System.out.println("written out metrics to " + outputFile.getCanonicalPath());

        String header = "time,count,throughput(msgs/s),max_latency(ms),mean_latency(ms),min_latency(ms)," +
                "stddev_latency(ms),p50_latency(ms),p75_latency(ms),p95_latency(ms),p98_latency(ms)," +
                "p99_latency(ms),p999_latency(ms)\n";

        boolean fileExists = outputFile.exists();
        if (!fileExists) {
            File parent = outputFile.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }
            outputFile.createNewFile();
        }

        try (FileWriter outputFileWriter = new FileWriter(outputFile, true)) {
            if (!fileExists) {
                outputFileWriter.append(header);
            }

            String time = new Date(System.currentTimeMillis()).toString();
            Snapshot snapshot = histogram.getSnapshot();
            double throughput = count * 1000.0 / (maxTime - minTime);

            String line = String.format("%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f%n",
                    time,
                    count,
                    throughput,
                    snapshot.getMax(),
                    snapshot.getMean(),
                    snapshot.getMin(),
                    snapshot.getStdDev(),
                    snapshot.getMedian(),
                    snapshot.get75thPercentile(),
                    snapshot.get95thPercentile(),
                    snapshot.get98thPercentile(),
                    snapshot.get99thPercentile(),
                    snapshot.get999thPercentile());

            outputFileWriter.append(line);
        }
    }
}
