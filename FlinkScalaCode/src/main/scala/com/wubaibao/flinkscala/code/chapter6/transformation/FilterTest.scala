package com.wubaibao.flinkscala.code.chapter6.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink Filter算子测试
 */
object FilterTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val stream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9)
    stream.filter(_ % 2 == 0).print()
    env.execute()
  }
}
