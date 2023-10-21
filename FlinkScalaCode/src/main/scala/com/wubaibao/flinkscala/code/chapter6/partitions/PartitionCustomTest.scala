package com.wubaibao.flinkscala.code.chapter6.partitions

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink自定义分区器
 */
object PartitionCustomTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val ds1: DataStream[String] = env.socketTextStream("node5", 9999)
    val ds2: DataStream[(String, Int)] = ds1.map(one => {
      val arr: Array[String] = one.split(",")
      (arr(0), arr(1).toInt)
    })

    val result: DataStream[(String, Int)] = ds2.partitionCustom(new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = {
        key.hashCode % numPartitions
      }
    }, _._1)

    result.print()
    env.execute()
  }
}
