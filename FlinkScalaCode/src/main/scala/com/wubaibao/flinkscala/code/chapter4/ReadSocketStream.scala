package com.wubaibao.flinkscala.code.chapter4

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object ReadSocketStream {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.disableOperatorChaining()

    //2.Scala 流处理导入隐式转换，使用Scala API 时需要隐式转换来推断函数操作后的类型
    import org.apache.flink.streaming.api.scala._

    //3.读取文件
    val ds: DataStream[String] = env.readTextFile("./data/words.txt")

    //4.进行wordCount统计
    ds.flatMap(line=>{line.split(" ")})
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
      .print()

    //5.最后使用execute 方法触发执行
    env.execute()
  }
}
