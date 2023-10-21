package com.wubaibao.flinkscala.code.chapter6.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink Kafka Sink 测试
 */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置隐式转换
    import org.apache.flink.api.scala._

    /**
     * Socket 中输入数据格式：
     * hello,world
     * hello,flink
     * hello,scala
     * hello,spark
     * hello,hadoop
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    //统计wordCount
    val result: DataStream[String] = ds.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .map(t => {
        t._1 + "-" + t._2
      })

    //准备KafkaSink对象
    val kafkaSink: KafkaSink[String] = KafkaSink.builder()
      .setBootstrapServers("node1:9092,node2:9092,node3:9092")
      //设置事务超时时间，最大不超过kafka broker的事务最大超时时间限制：max.transaction.timeout.ms
      .setProperty("transaction.timeout.ms", 15 * 60 * 1000L + "")
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("flink-topic")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .build()

    //将结果写入Kafka
    result.sinkTo(kafkaSink)
    env.execute()
  }

}
