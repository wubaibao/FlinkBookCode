package com.wubaibao.flinkjava.code.chapter6.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 读取Kafka的数据，只读取Value
 */
public class KafkaSourceWithoutKeyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092") //设置Kafka 集群节点
                .setTopics("testtopic") //设置读取的topic
                .setGroupId("my-test-group") //设置消费者组
                .setStartingOffsets(OffsetsInitializer.latest()) //设置读取数据位置
                .setValueOnlyDeserializer(new SimpleStringSchema()) //设置value的序列化格式
                .build();

        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "kafka-source");

        kafkaDS.print();

        env.execute();
    }
}
