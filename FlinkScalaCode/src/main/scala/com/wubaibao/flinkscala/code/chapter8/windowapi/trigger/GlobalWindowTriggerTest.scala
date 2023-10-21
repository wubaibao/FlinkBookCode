package com.wubaibao.flinkscala.code.chapter8.windowapi.trigger

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Flink Window API - GlobalWindow 自定义触发器
 * 案例：读取基站日志数据，手动指定trigger触发器，每个基站数据每5秒生成窗口并触发计算。
 */
object GlobalWindowTriggerTest {
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
      .window(GlobalWindows.create())
      .trigger(new MyTimeTrigger1())
      .process(new ProcessWindowFunction[StationLog, String, String, GlobalWindow] {
        override def process(key: String, context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit = {
          //统计每个基站所有主叫通话总时长
          var sumCallTime = 0L
          for (elem <- elements) {
            sumCallTime += elem.duration
          }

          out.collect("基站：" + key + ",所有主叫通话总时长：" + sumCallTime)
        }
      }).print()

    env.execute()
  }
}

//MyCountTrigger触发器，每3条数据触发一次计算
class MyTimeTrigger1 extends Trigger[StationLog, GlobalWindow] {
  //创建状态描述符，该状态标记当前key是否有对应的定时器
  val timerStateDescriptor = new ValueStateDescriptor[Boolean]("timer-state", classOf[Boolean])

  //每来一条数据，都会调用一次
  override def onElement(element: StationLog,
                         timestamp: Long,
                         window: GlobalWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {

    //获取当前窗口中定时器是否存在的状态
    val isExist: Boolean = ctx.getPartitionedState(timerStateDescriptor).value()
    if (isExist == null || !isExist) {
      //注册一个基于事件时间的定时器，延迟5秒触发
      ctx.registerEventTimeTimer(timestamp + 4999L)
      //更新状态
      ctx.getPartitionedState(timerStateDescriptor).update(true)
    }

    TriggerResult.CONTINUE
  }

  //注册处理时间定时器。如果基于ProcessTime处理，在onElement方法中注册了定时器，当定时器触发时，会调用onProcessingTime方法
  override def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  //注册事件时间定时器。如果基于EventTime处理，在onElement方法中注册了定时器，当定时器触发时，会调用onEventTime方法
  override def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //更新状态为false
    ctx.getPartitionedState(timerStateDescriptor).update(false);
    TriggerResult.FIRE_AND_PURGE
  }

  //clear() 方法处理在对应窗口被移除时所需的逻辑。
  override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.getPartitionedState(timerStateDescriptor).clear()
  }
}
