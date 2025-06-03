/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.flinkbench.datasource;

import com.intel.hibench.flinkbench.util.FlinkBenchConfig;
import java.io.IOException;
import java.util.Properties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public abstract class StreamBase {

  private KafkaSource<Tuple2<String, String>> dataStream;

  public KafkaSource<Tuple2<String, String>> getDataStream() {
    return this.dataStream;
  }

  public void createDataStream(FlinkBenchConfig config) throws Exception {

    Properties properties = new Properties();
    properties.setProperty("zookeeper.connect", config.zkHost);
    properties.setProperty("group.id", config.consumerGroup);
    properties.setProperty("bootstrap.servers", config.brokerList);
    properties.setProperty("auto.offset.reset", config.offsetReset);

    this.dataStream =
        KafkaSource.<Tuple2<String, String>>builder()
            .setDeserializer(
                new KafkaRecordDeserializationSchema<Tuple2<String, String>>() {
                  @Override
                  public void deserialize(
                      ConsumerRecord<byte[], byte[]> record, Collector<Tuple2<String, String>> out)
                      throws IOException {
                    out.collect(new Tuple2<>(new String(record.key()), new String(record.value())));
                  }

                  @Override
                  public TypeInformation<Tuple2<String, String>> getProducedType() {
                    return new TupleTypeInfo<>(
                        TypeInformation.of(String.class), TypeInformation.of(String.class));
                  }
                })
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setProperties(properties)
            .setTopics(config.topic)
            .build();
  }

  public void processStream(FlinkBenchConfig config) throws Exception {}
}
