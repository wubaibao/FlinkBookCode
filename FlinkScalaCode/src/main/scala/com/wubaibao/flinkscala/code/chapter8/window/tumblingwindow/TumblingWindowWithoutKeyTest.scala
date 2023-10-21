package com.wubaibao.flinkscala.code.chapter8.window.tumblingwindow

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Flink 基于EventTime TumblingWindow 滚动窗口测试
 * 案例：读取基站日志数据，每隔10s统计所有主叫通话总时长。
 */
object TumblingWindowWithoutKeyTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    /**
     * Socket中输入数据格式如下：
     * 001,181,182,busy,1000,10
     * 002,182,183,fail,3000,20
     * 001,183,184,busy,2000,30
     * 002,184,185,busy,6000,40
     * 003,181,183,busy,5000,50
     * 001,181,182,busy,7000,10
     * 002,182,183,fail,9000,20
     * 001,183,184,busy,11000,30
     * 002,184,185,busy,6000,40
     * 003,181,183,busy,12000,50
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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new ProcessAllWindowFunction[StationLog, String, TimeWindow] {
        override def process(context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit = {
          //获取窗口起始时间
          val startTime: Long = context.window.getStart
          val endTime: Long = context.window.getEnd
          //统计每个基站所有主叫通话总时长
          var sumCallTime = 0L
          for (elem <- elements) {
            sumCallTime += elem.duration
          }

          out.collect("窗口范围：[" + startTime + "~" + endTime + "),所有基站主叫通话总时长：" + sumCallTime)

        }
      }).print()

    env.execute()
  }

}
