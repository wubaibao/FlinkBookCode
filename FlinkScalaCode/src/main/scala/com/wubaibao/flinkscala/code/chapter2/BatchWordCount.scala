package com.wubaibao.flinkscala.code.chapter2

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 *  Scala : Flink Dataset 批处理WordCount
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {

    //1.准备环境，注意是Scala中对应的Flink环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.Scala 批处理导入隐式转换，使用Scala API 时需要隐式转换来推断函数操作后的类型
    import org.apache.flink.api.scala._

    //3.读取数据文件
    val linesDS: DataSet[String] = env.readTextFile("./data/words.txt")

    //4.进行 WordCount 统计并打印
    linesDS.flatMap(line => {
      line.split(" ")
    })
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }

}
