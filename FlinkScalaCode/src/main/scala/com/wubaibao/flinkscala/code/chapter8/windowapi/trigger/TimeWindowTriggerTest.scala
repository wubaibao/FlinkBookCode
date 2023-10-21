package com.wubaibao.flinkscala.code.chapter8.windowapi.trigger

import com.wubaibao.flinkscala.code.chapter6.StationLog
import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Flink Window API - 时间窗口自定义触发器
 * 案例：读取基站日志数据，手动指定trigger触发器，每个基站每条数据都触发窗口执行。
 */
object TimeWindowTriggerTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    /**
     * Socket中输入数据格式如下：
     */
    val sourceDS: DataStream[String] = env.socketTextStream("node5", 9999)

    //将数据转换成StationLog对象
    val stationLogDS: DataStream[StationLog] = sourceDS.map(line => {
      val arr = line.split(",")
      StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
    })

    //给 stationLogDS 设置水位线
    val dsWithWatermark: DataStream[StationLog] = stationLogDS.assignTimestampsAndWatermarks(
      //设置水位线策略
      WatermarkStrategy.forBoundedOutOfOrderness[StationLog](Duration.ofSeconds(2))
        //设置事件时间抽取器
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(element: StationLog, recordTimestamp: Long): Long = {
            element.callTime
          }
        })
        //设置并行度空闲时间，方便推进水位线
        .withIdleness(Duration.ofSeconds(5))
    )

    //按照基站id进行分组，每隔5s窗口统计每个基站所有主叫通话总时长
    dsWithWatermark
      .keyBy(_.sid)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .trigger(new MyTimeTrigger2())
      .process(new ProcessWindowFunction[StationLog,String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit = {
          //获取窗口起始时间
          val startTime: Long = if(context.window.getStart <0) 0 else context.window.getStart
          val endTime: Long = context.window.getEnd
          //统计每个基站所有主叫通话总时长
          var sumCallTime = 0L
          for (elem <- elements) {
            sumCallTime += elem.duration
          }
          out.collect("窗口范围：[" + startTime + "~" + endTime + "),基站：" + key + ",所有主叫通话总时长：" + sumCallTime)
        }
      }).print()

    env.execute()
  }

}

//MyTimeTrigger2()，来一条数据触发一次窗口计算
class MyTimeTrigger2 extends Trigger[StationLog, TimeWindow] {

  override def onElement(element: StationLog, timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
    println("onElement方法执行...")
    //只要来一条数据就触发一次窗口
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
    println("onProcessingTime方法执行，窗口触发计算...")
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
    println("onEventTime方法执行，窗口触发计算...")
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: TriggerContext): Unit = {
    println("clear方法执行，窗口销毁...")
    //这里没有状态，不需要清空
  }
}