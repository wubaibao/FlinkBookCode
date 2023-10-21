package com.wubaibao.flinkscala.code.chapter3

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Scala 使用Flink本地WebUI
 */
object FlinkLocalWebUI {
  def main(args: Array[String]): Unit = {

    //1.创建本地WebUI环境
    val configuration = new Configuration()
    //设置绑定的本地端口
    configuration.set(RestOptions.BIND_PORT,"8081")
    //第一种设置方式
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)

    //2.Scala 流处理导入隐式转换，使用Scala API 时需要隐式转换来推断函数操作后的类型
    import org.apache.flink.streaming.api.scala._

    //3.读取Socket数据
    val linesDS: DataStream[String] = env.socketTextStream("node5", 9999)

    //4.进行WordCount统计
    linesDS.flatMap(line=>{line.split(",")})
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
      .print()

    //5.最后使用execute 方法触发执行
    env.execute()

  }

}
