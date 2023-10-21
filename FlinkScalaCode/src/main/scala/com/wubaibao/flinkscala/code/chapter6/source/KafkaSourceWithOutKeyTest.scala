package com.wubaibao.flinkscala.code.chapter6.source

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink 读取Kafka的数据，只读取Value
 */
object KafkaSourceWithOutKeyTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val kafkaSource: KafkaSource[String] = KafkaSource.builder[String]()
      .setBootstrapServers("node1:9092,node2:9092,node3:9092") //设置Kafka集群Brokers
      .setTopics("testtopic") //设置topic
      .setGroupId("my-test-group") //设置消费者组
      .setStartingOffsets(OffsetsInitializer.latest()) // 读取位置
      .setValueOnlyDeserializer(new SimpleStringSchema()) //设置Value的反序列化格式
      .build()

    val kafkaDS: DataStream[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")

    kafkaDS.print()

    env.execute()
  }
}
