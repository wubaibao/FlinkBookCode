package com.wubaibao.flinkscala.code.chapter8.watermark

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
 * Flink 自定义Watermark - PunctuatedWatermarkGenerator
 * 案例：读取Socket基站日志数据，按照基站id进行分组，每隔5s窗口统计每个基站所有主叫通话总时长
 * 这里实现 PunctuatedWatermark 以基站id 001的事件时间生成Watermark
 */
object PunctuatedWatermarkGeneratorTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //    env.setParallelism(1)

    /**
     * Socket中输入数据格式如下：
     * 001,181,182,busy,1000,10
     * 004,184,185,busy,4000,40
     * 005,181,183,busy,5000,50
     * 002,182,183,fail,2000,20
     * 001,181,185,success,6000,60
     * 003,182,184,busy,7000,30
     * 001,183,184,busy,7000,30
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
      WatermarkStrategy.forGenerator[StationLog](new WatermarkGeneratorSupplier[StationLog]{
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[StationLog] = {
          new CustomPunctuatedWatermark()
        }
      } )
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
      .sum("duration")
      .print()

    env.execute()
  }

}

//自定义PunctuatedWatermark实现
class CustomPunctuatedWatermark extends WatermarkGenerator[StationLog] {
  //定义最大延迟时间
  val maxDelayTime = 2000L

  var currentMaxTimeStamp = Long.MinValue + maxDelayTime + 1L
  //定义水位线
  override def onEvent(stationLog: StationLog, eventTimestamp: Long, watermarkOutput: WatermarkOutput): Unit = {
    //如果是基站id为001的事件，就生成水位线
    if ("001".equals(stationLog.sid) ) {
      currentMaxTimeStamp = Math.max(currentMaxTimeStamp,stationLog.callTime)
      //生成水位线
      watermarkOutput.emitWatermark(new Watermark(currentMaxTimeStamp - maxDelayTime - 1L))
    }
  }

  //定义空闲时间
  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    //这里什么逻辑都不需要实现，watermark是基于事件来生成的，不是周期性生成的，已经在onEvent方法中生成了

  }
}
