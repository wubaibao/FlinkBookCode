package com.wubaibao.flinkscala.code.chapter6.source

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * 读取Kafka 中数据key和value
 */
object KafkaSourceWithKeyTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val kafkaSource: KafkaSource[(String, String)] = KafkaSource.builder[(String, String)]()
      .setBootstrapServers("node1:9092,node2:9092,node3:9092") //设置Kafka集群Brokers
      .setTopics("testtopic") //设置topic
      .setGroupId("my-test-group") //设置消费者组
      .setStartingOffsets(OffsetsInitializer.latest()) // 读取位置
      .setDeserializer(new KafkaRecordDeserializationSchema[(String, String)] {
        //组织 consumerRecord 数据
        override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], collector: Collector[(String, String)]): Unit = {
          var key: String = null
          var value: String = null
          if (consumerRecord.key() != null) {
            key = new String(consumerRecord.key(), "UTF-8")
          }
          if (consumerRecord.value() != null) {
            value = new String(consumerRecord.value(), "UTF-8")
          }

          collector.collect((key, value))
        }

        //设置返回的二元组类型 ,createTuple2TypeInformation 需要导入隐式转换
        override def getProducedType: TypeInformation[(String, String)] = {
          createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
        }
      })
      .build()

    val ds: DataStream[(String, String)] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
    ds.print()
    env.execute()
  }

}
