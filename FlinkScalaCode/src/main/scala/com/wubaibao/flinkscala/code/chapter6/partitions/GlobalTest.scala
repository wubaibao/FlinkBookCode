package com.wubaibao.flinkscala.code.chapter6.partitions

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink Global全局分区测试
 */
object GlobalTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 导入隐式转换
    import org.apache.flink.api.scala._
    val ds: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    ds.print("普通打印")
    // 设置全局分区
    ds.global.print("global")
    env.execute()
  }

}
