package com.wubaibao.flinkscala.code.chapter6.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink iterate 算子测试
 */
object IterateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._

    val ds1: DataStream[String] = env.socketTextStream("node5", 9999)

    //转换数据流
    val ds2: DataStream[Int] = ds1.map(v => v.toInt)
    //定义迭代流，并指定迭代体和迭代条件
    val result: DataStream[Int] = ds2.iterate(
      iteration => {
        //定义迭代体
        val minusOne: DataStream[Int] = iteration.map(v => {println("迭代体中value值为："+v);v - 1})

        //定义迭代条件，满足的继续迭代
        val stillGreaterThanZero: DataStream[Int] = minusOne.filter(_ > 0)

        //定义哪些数据最后进行输出
        val lessThanZero: DataStream[Int] = minusOne.filter(_ <= 0)

        //返回tuple，第一个参数是哪些数据流继续迭代，第二个参数是哪些数据流进行输出
        (stillGreaterThanZero, lessThanZero)
      })

    //打印最后结果
    result.print()

    env.execute()
  }

}
