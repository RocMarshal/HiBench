package com.intel.hibench.flinkbench.microbench;

import com.intel.hibench.common.streaming.UserVisitParser;
import com.intel.hibench.common.streaming.metrics.KafkaReporter;
import com.intel.hibench.flinkbench.datasource.StreamBase;
import com.intel.hibench.flinkbench.util.FlinkBenchConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FixedWindow extends StreamBase {

  @Override
  public void processStream(final FlinkBenchConfig config) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setBufferTimeout(config.bufferTimeout);
    env.enableCheckpointing(config.checkpointDuration);

    createDataStream(config);
    DataStream<Tuple2<String, String>> dataStream =
        env.fromSource(getDataStream(), WatermarkStrategy.noWatermarks(), "source");
    long windowDuration = Long.parseLong(config.windowDuration);
    long windowSlideStep = Long.parseLong(config.windowSlideStep);

    dataStream
        .map(
            (MapFunction<Tuple2<String, String>, Tuple2<String, Tuple2<Long, Integer>>>)
                value -> {
                  String ip = UserVisitParser.parse(value.f1).getIp();
                  return new Tuple2<>(ip, new Tuple2<>(Long.parseLong(value.f0), 1));
                })
        .keyBy(t -> t.f0)
        .timeWindow(Time.milliseconds(windowDuration), Time.milliseconds(windowSlideStep))
        .reduce(
            (ReduceFunction<Tuple2<String, Tuple2<Long, Integer>>>)
                (v1, v2) ->
                    new Tuple2<>(
                        v1.f0, new Tuple2<>(Math.min(v1.f1.f0, v2.f1.f0), v1.f1.f1 + v2.f1.f1)))
        .map(
            (MapFunction<Tuple2<String, Tuple2<Long, Integer>>, String>)
                value -> {
                  KafkaReporter kafkaReporter =
                      new KafkaReporter(config.reportTopic, config.brokerList);
                  for (int i = 0; i < value.f1.f1; i++) {
                    kafkaReporter.report(value.f1.f0, System.currentTimeMillis());
                  }
                  return value.f0;
                });

    env.execute("Fixed Window Job");
  }
}
