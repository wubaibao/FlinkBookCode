package com.wubaibao.flinkscala.code.chapter6.sink

import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink 写出数据到Kafka,有key value
 */
object KafkaSinkWithKeyValueTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val result: DataStream[(String, Int)] = env.socketTextStream("node5", 9999)
      .flatMap(_.split(","))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    //准备Kafka Sink 对象
    val kafkaSink: KafkaSink[(String, Int)] = KafkaSink.builder[(String, Int)]()
      .setBootstrapServers("node1:9092,node2:9092,node3:9092")
      .setProperty("transaction.timeout.ms", 15 * 60 * 1000L + "")
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic("flink-topic-2")
          .setKeySerializationSchema(new MyKeySerializationSchema())
          .setValueSerializationSchema(new MyValueSerializationSchema())
          .build()
      ).setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .build()

    result.sinkTo(kafkaSink)

    env.execute()
  }


}

class MyKeySerializationSchema() extends SerializationSchema[(String,Int)]{
  override def serialize(t: (String, Int)): Array[Byte] = t._1.getBytes()
}
class MyValueSerializationSchema() extends SerializationSchema[(String,Int)]{
  override def serialize(t: (String, Int)): Array[Byte] = t._2.toString.getBytes()
}