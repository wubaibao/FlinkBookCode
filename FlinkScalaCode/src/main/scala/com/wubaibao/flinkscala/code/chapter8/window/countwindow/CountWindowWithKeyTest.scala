package com.wubaibao.flinkscala.code.chapter8.window.countwindow

import com.wubaibao.flinkscala.code.chapter6.StationLog
import com.wubaibao.flinkscala.code.chapter8.window.globalwindow.MyCountTrigger
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * CountWindow 计数窗口测试
 * 案例：读取基站日志数据，每个基站ID每5条数据触发一次计算。
 */
object CountWindowWithKeyTest {
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
     * 001,181,183,busy,5000,50
     * 001,181,182,busy,7000,10
     * 002,182,183,fail,9000,20
     * 001,183,184,busy,11000,30
     * 002,184,185,busy,6000,40
     * 002,181,183,busy,12000,50
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
      .countWindow(5)
      .process(new ProcessWindowFunction[StationLog, String, String, GlobalWindow] {
        override def process(key: String, context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit = {
          //统计每个基站所有主叫通话总时长
          var sumCallTime = 0L
          for (elem <- elements) {
            sumCallTime += elem.duration
          }

          out.collect("基站：" + key + ",近5条通话总时长：" + sumCallTime)
        }
      }).print()

    env.execute()

  }
}
