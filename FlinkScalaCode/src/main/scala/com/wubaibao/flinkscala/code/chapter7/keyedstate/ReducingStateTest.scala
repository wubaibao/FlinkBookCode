package com.wubaibao.flinkscala.code.chapter7.keyedstate

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * Flink ReducingState 测试
 * 案例：读取基站通话数据，每隔20s统计每个主叫号码通话总时长
 */
object ReducingStateTest {
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

    // 对ds进行转换处理，得到StationLog对象
    val stationLogDS: DataStream[StationLog] = ds.map((line: String) => {
      val arr: Array[String] = line.split(",")
      StationLog(
        arr(0).trim,
        arr(1).trim,
        arr(2).trim,
        arr(3).trim,
        arr(4).toLong,
        arr(5).toLong
      )
    })

    val result: DataStream[String] = stationLogDS
      .keyBy(_.callOut)
      .process(new KeyedProcessFunction[String, StationLog, String] {
        // 定义 ReducingState 状态，存储通话总时长
        private var callDurationState: ReducingState[Long] = _

        override def open(parameters: Configuration): Unit = {
          // 定义状态描述器
          val rsd = new ReducingStateDescriptor[Long]("callDuration-state", new ReduceFunction[Long] {
            override def reduce(value1: Long, value2: Long): Long = value1 + value2
          }, classOf[Long])

          // 根据状态描述器获取状态
          callDurationState = getRuntimeContext.getReducingState(rsd)
        }

        override def processElement(stationLog: StationLog, ctx: KeyedProcessFunction[String, StationLog, String]#Context, out: Collector[String]): Unit = {
          // 获取当前主叫号码状态中的通话时长
          val totalCallTime = callDurationState.get

          if (totalCallTime == 0) {
            // 获取当前处理数据时间
            val time = ctx.timerService.currentProcessingTime
            // 注册一个20s后的定时器
            ctx.timerService.registerProcessingTimeTimer(time + 20 * 1000L)
          }

          // 将通话时长存入ReducingState
          callDurationState.add(stationLog.duration)
        }

        /**
         * 定时器触发时，调用该方法
         */
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
          // 输出结果
          out.collect(s"主叫号码：${ctx.getCurrentKey}，近20s通话时长：${callDurationState.get}")

          // 清空状态
          callDurationState.clear()
        }
      })

    result.print()
    env.execute()
  }

}
