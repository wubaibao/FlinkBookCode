package com.wubaibao.flinkscala.code.chapter6.partitions

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object RescaleTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    //导入隐式转换
    import org.apache.flink.api.scala._

    val ds: DataStream[Object] = env.addSource[Object](new RichParallelSourceFunction[Object] {

      var isRunning = true

      override def run(ctx: SourceFunction.SourceContext[Object]): Unit = {
        val list1 = List[String]("a", "b", "c", "d", "e", "f")
        val list2 = List[Integer](1, 2, 3, 4, 5, 6)
        list1.foreach(one => {
          if (0 == getRuntimeContext.getIndexOfThisSubtask) {
            ctx.collect(one)
          }
        })
        list2.foreach(one => {
          if (1 == getRuntimeContext.getIndexOfThisSubtask) {
            ctx.collect(one)
          }
        })
      }

      override def cancel(): Unit = {
        isRunning = false
      }

    })

    ds.rescale.print("rescale").setParallelism(4)
    env.execute()

  }

}
