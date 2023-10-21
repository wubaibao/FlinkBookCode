package com.wubaibao.flinkscala.code.chapter6.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink Union 算子测试
 */
object UnionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._
    val ds1: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4))
    val ds2: DataStream[Int] = env.fromCollection(List(5, 6, 7, 8))
    ds1.union(ds2).print()
    env.execute()

  }

}
