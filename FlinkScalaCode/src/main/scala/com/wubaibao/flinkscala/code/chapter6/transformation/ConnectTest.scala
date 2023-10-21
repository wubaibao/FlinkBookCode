package com.wubaibao.flinkscala.code.chapter6.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink Connect 算子测试
 */
object ConnectTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._
    val ds1: DataStream[(String, Int)] = env.fromCollection(List(("a", 1), ("b", 2), ("c", 3)))
    val ds2: DataStream[String] = env.fromCollection(List("aa","bb","cc"))
    //connect连接两个流，两个流的数据类型可以不一样
    val result: DataStream[(String, Int)] =
      ds1.connect(ds2).map(tp => tp, value => {(value, 1)})

    result.print()
    env.execute()
  }

}
