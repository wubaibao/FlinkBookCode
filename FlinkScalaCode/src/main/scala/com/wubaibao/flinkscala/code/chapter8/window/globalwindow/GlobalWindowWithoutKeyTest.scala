package com.wubaibao.flinkscala.code.chapter8.window.globalwindow

import com.wubaibao.flinkscala.code.chapter6.StationLog
import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Flink 基于EventTime GlobalWindow 全局窗口测试
 * 案例：读取基站日志数据，手动指定trigger触发器，所有基站只要有3条数据触发一次计算。
 */
object GlobalWindowWithoutKeyTest {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  //导入隐式转换
  import org.apache.flink.streaming.api.scala._
  /**
   * Socket中输入数据格式如下：
   * 001,181,182,busy,1000,10
   * 002,182,183,fail,3000,20
   * 001,183,184,busy,2000,30
   * 002,184,185,busy,6000,40
   * 002,181,183,busy,5000,50
   * 001,181,182,busy,7000,10
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
    .windowAll(GlobalWindows.create())
    .trigger(new MyGlobalCountTrigger())
    .process(new ProcessAllWindowFunction[StationLog, String, GlobalWindow] {
      override def process(context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit = {
        //统计每个基站所有主叫通话总时长
        var sumCallTime = 0L
        for (elem <- elements) {
          sumCallTime += elem.duration
        }

        out.collect("全局窗口触发,近3条通话总时长：" + sumCallTime)
      }
    }).print()

  env.execute()
}

//MyGlobalCountTrigger() 每隔3条数据触发一次计算
class MyGlobalCountTrigger extends Trigger[StationLog, GlobalWindow] {
  //设置 ValueStateDescriptor描述器，用于存储计数器
  private val eventCountDescriptor = new ValueStateDescriptor[Long]("event-count", classOf[Long])

  //每来一条数据，都会调用一次
  override def onElement(element: StationLog,
                         timestamp: Long,
                         window: GlobalWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {
    //获取状态计数器的值
    val eventState = ctx.getPartitionedState(eventCountDescriptor)
    //每来一条数据，状态值加1，初始状态值为null,直接返回1即可
    val count = Option(eventState.value()).getOrElse(0L) + 1L
    //将计数器的值存入状态中
    eventState.update(count)

    //如果计数器的值等于3，触发计算，并清空计数器
    if (count == 3L) {
      //清空状态计数
      eventState.clear()
      //触发计算
      TriggerResult.FIRE_AND_PURGE
    } else {
      //如果状态计数器的值不等于3，不触发计算
      TriggerResult.CONTINUE
    }
  }

  //注册处理时间定时器。如果基于ProcessTime处理，在onElement方法中注册了定时器，当定时器触发时，会调用onProcessingTime方法
  override def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  //注册事件时间定时器。如果基于EventTime处理，在onElement方法中注册了定时器，当定时器触发时，会调用onEventTime方法
  override def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  //clear() 方法处理在对应窗口被移除时所需的逻辑。
  override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.getPartitionedState(eventCountDescriptor).clear()
  }
}