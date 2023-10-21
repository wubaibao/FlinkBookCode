package com.wubaibao.flinkscala.code.chapter6.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink Map算子测试
 */
object MapTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[Int] = env.fromCollection(List(1, 3, 5, 7))
    val result: DataStream[Int] = ds.map(_ + 1)
    result.print()
    env.execute()
  }
}
