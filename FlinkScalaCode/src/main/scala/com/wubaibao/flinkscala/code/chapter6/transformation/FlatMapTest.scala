package com.wubaibao.flinkscala.code.chapter6.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink FlatMap算子测试
 */
object FlatMapTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._
    val ds: DataStream[String] = env.fromCollection(List("1,2", "3,4", "5,6", "7,8"))
    ds.flatMap(_.split(",")).print()
    env.execute()
  }
}
