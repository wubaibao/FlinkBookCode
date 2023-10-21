package com.wubaibao.flinkscala.code.chapter7.savepoints

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink SavePoint 状态恢复测试
 */
object SavePoitTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    // 读取socket数据做wordcount
    env.socketTextStream("node5", 9999).uid("socket-source")
      .flatMap(_.split(" ")).uid("flatMap")
      .map((_, 1)).uid("map")
      .keyBy(_._1)
      .sum(1).uid("sum")
      .print().uid("print")

    env.execute()

  }
}
