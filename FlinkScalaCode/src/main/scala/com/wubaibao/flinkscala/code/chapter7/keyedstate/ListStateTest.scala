package com.wubaibao.flinkscala.code.chapter7.keyedstate

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.util

/**
 * Flink ListState 测试
 * 案例：读取基站通话数据，每隔20s统计每个主叫号码通话总时长
 */
object ListStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    /**
     * Socket中数据如下:
     * 001,186,187,busy,1000,10
     * 002,187,186,fail,2000,20
     * 003,186,188,busy,3000,30
     * 004,187,186,busy,4000,40
     * 005,189,187,busy,5000,50
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    //对ds进行转换处理，得到StationLog对象
    val stationLogDS: DataStream[StationLog] = ds.map(new MapFunction[String, StationLog] {
      override def map(line: String): StationLog = {
        val arr: Array[String] = line.split(",")
        StationLog(
          arr(0).trim,
          arr(1).trim,
          arr(2).trim,
          arr(3).trim,
          arr(4).toLong,
          arr(5).toLong
        )
      }
    })

    //这里要用到 定时器，所以采用process API
    val result: DataStream[String] = stationLogDS
      .keyBy(_.callOut)
      .process(new KeyedProcessFunction[String, StationLog, String]() {
        //定义一个ListState用来存放同一个主叫号码的所有通话时长
        private var listState: ListState[Long] = _

        override def open(parameters: Configuration): Unit = {
          //定义状态描述器
          val listStateDescriptor: ListStateDescriptor[Long] = new ListStateDescriptor[Long]("listState", classOf[Long])
          //获取ListState
          listState = getRuntimeContext.getListState(listStateDescriptor)
        }

        override def processElement(value: StationLog, ctx: KeyedProcessFunction[String, StationLog, String]#Context, out: Collector[String]): Unit = {
          //获取ListState中的数据
          val iterable: java.lang.Iterable[Long] = listState.get()
          //如果iterable中没有数据，说明是第一条数据，需要注册一个定时器
          if (!iterable.iterator().hasNext) {
            //获取当前处理数据时间
            val time: Long = ctx.timerService().currentProcessingTime()
            //注册一个20s后的定时器
            ctx.timerService().registerProcessingTimeTimer(time + 20 * 1000L)
          }

          //将通话时长存入ListState
          listState.add(value.duration)
        }

        /**
         * 定时器触发时，对同一个主叫号码的所有通话时长进行求和
         */
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
          //获取ListState中的所有通话时长
          val iterable: java.lang.Iterable[Long] = listState.get()

          //定义一个变量，用来存放通话时长总和
          var totalCallTime: Long = 0L

          //遍历迭代器，对通话时长进行求和
          val iterator: util.Iterator[Long] = iterable.iterator()
          while (iterator.hasNext) {
            totalCallTime += iterator.next()
          }
          //输出结果
          out.collect("主叫号码：" + ctx.getCurrentKey + "，近20s通话时长：" + totalCallTime)

          //清空ListState
          listState.clear()
        }
      })

    result.print()
    env.execute()
  }
}
