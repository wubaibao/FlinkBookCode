package com.wubaibao.flinkscala.code.chapter8.eventtimedsoperator

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Flink EventTime下union操作
 * 案例：读取socket中数据流形成两个流，进行关联后设置窗口，每隔5秒统计每个基站通话次数。
 */
object UnionOnEventTimeTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //为了方便看出效果，这里设置并行度为1
    env.setParallelism(1)
    //设置隐式转换
    import org.apache.flink.streaming.api.scala._
    //读取socket中数据流形成两个流
    val ADS: DataStream[StationLog] = env.socketTextStream("node5", 8888).map(line => {
      val arr = line.split(",")
      StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
    })

    //给 stationLogDS 设置水位线
    val adsWithWatermark: DataStream[StationLog] = ADS.assignTimestampsAndWatermarks(
      //设置水位线策略
      WatermarkStrategy.forBoundedOutOfOrderness[StationLog](Duration.ofSeconds(2))
        //设置事件时间抽取器
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(element: StationLog, recordTimestamp: Long): Long = {
            element.callTime
          }
        })
    )
    val BDS: DataStream[StationLog] = env.socketTextStream("node5", 9999).map(line => {
      val arr = line.split(",")
      StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
    })

    //给 stationLogDS 设置水位线
    val bdsWithWatermark: DataStream[StationLog] = BDS.assignTimestampsAndWatermarks(
      //设置水位线策略
      WatermarkStrategy.forBoundedOutOfOrderness[StationLog](Duration.ofSeconds(2))
        //设置事件时间抽取器
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(element: StationLog, recordTimestamp: Long): Long = {
            element.callTime
          }
        })
    )

    //两流进行union关联
    adsWithWatermark.union(bdsWithWatermark)
      .keyBy(_.sid)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new ProcessWindowFunction[StationLog,String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit = {
          println("window-watermark:" + context.currentWatermark)
          //获取窗口起始时间
          val startTime: Long = if(context.window.getStart<0) 0 else context.window.getStart
          //获取窗口结束时间
          val endTime: Long = context.window.getEnd
          //统计窗口内的通话次数
          val count: Int = elements.size
          //输出结果
          out.collect("窗口范围：[" + startTime + "~" + endTime + "),基站：" + key + ",通话总次数：" + count)

        }
      }).print()
    env.execute()
  }

}
