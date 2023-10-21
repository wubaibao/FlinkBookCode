package com.wubaibao.flinkscala.code.chapter6.partitions

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink Forward 测试
 */
object ForwardTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    val ds1: DataStream[Integer] = env.addSource(new RichParallelSourceFunction[Integer] {
      var flag = true

      override def run(ctx: SourceFunction.SourceContext[Integer]): Unit = {
        val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        for (elem <- list) {
          val subtask: Int = getRuntimeContext.getIndexOfThisSubtask
          if (elem % 2 != 0 && 0 == subtask) {
            ctx.collect(elem)

          } else if (elem % 2 == 0 && 1 == subtask) {
            ctx.collect(elem)
          }
        }

      }

      override def cancel(): Unit = {
        flag = false
      }

    })

    ds1.print("ds1")
    val ds2: DataStream[String] = ds1.forward.map(one => {
      one + "xx"
    })
    ds2.print("ds2")
    env.execute()

  }

}
