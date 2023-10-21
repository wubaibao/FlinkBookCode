package com.wubaibao.flinkscala.code.chapter6.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink KeyBy算子测试
 */
object KeyByTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._

    val ds: DataStream[(String, Int)] = env.fromCollection(List(("a", 1),
      ("b", 2),
      ("c", 3),
      ("a", 4),
      ("b", 5)))

    ds.keyBy(tp=>{tp._1}).sum(1).print()
    env.execute()
  }

}
