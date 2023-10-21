package com.wubaibao.flinkscala.code.chapter6.partitions

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink Reblance 重平衡分区测试
 */
object ReblanceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    ds.rebalance.print().setParallelism(3)
    env.execute()
  }

}
