package com.wubaibao.flinkscala.code.chapter8.watermark

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.time.Duration

/**
 * Flink watermark 对齐测试
 *  Socket 中输入数据格式如下：
 *  001,181,182,busy,1000,10
 *  001,184,185,busy,2000,40
 *  001,181,183,busy,3000,50
 *  001,182,183,fail,4000,20
 *  001,181,185,success,5000,60
 */
object WatermarkAlignmentTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    /**
     * Socket中输入数据格式如下：
     * 001,181,182,busy,1000,10
     * 002,182,183,fail,2000,20
     * 003,183,184,busy,3000,30
     * 004,184,185,busy,4000,40
     * 005,181,183,busy,5000,50
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
      WatermarkStrategy.forMonotonousTimestamps[StationLog]()
        //设置事件时间抽取器
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(element: StationLog, recordTimestamp: Long): Long = {
            element.callTime
          }
        })
        //设置Watermark对齐,对齐组为socket-source-group，watermark最大偏移值为5s，检查周期是2s
        .withWatermarkAlignment("socket-source-group",Duration.ofSeconds(5 ),Duration.ofSeconds(2))
    )

    dsWithWatermark.print()
    env.execute()

  }
}
