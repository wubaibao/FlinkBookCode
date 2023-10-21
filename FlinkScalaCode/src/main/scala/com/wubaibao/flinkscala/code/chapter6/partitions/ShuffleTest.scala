package com.wubaibao.flinkscala.code.chapter6.partitions

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink Shuffle 随机分区测试
 */
object ShuffleTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度为1
    env.setParallelism(1)

    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    ds.shuffle.print().setParallelism(3)
    env.execute()


  }

}
