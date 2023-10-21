package com.wubaibao.flinkscala.code.chapter8.eventtimedsoperator

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Flink - IntervalJoin
 * 案例：读取用户登录流和广告点击流，通过Interval Join分析用户点击广告的行为
 */
object IntervalJoinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度为1，方便测试
    env.setParallelism(1)
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    /**
     * 读取socket中用户登录流，并对用户登录流设置watermark
     * 用户登录流数据格式: 用户ID,登录时间
     * user_1,1000
     */
    val loginDS: DataStream[String] = env.socketTextStream("node5", 8888)
    // 设置水位线
    val loginDSWithWatermark: DataStream[String] = loginDS.assignTimestampsAndWatermarks(
      // 设置watermark ,延迟时间为2s
      WatermarkStrategy
        .forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(element: String, recordTimestamp: Long): Long = {
            element.split(",")(1).toLong
          }
        })
    )

    /**
     * 读取socket中广告点击流，并对广告点击流设置watermark
     * 广告点击流数据格式: 用户ID,广告ID,点击时间
     * user_1,product_1,1000
     */
    val clickDS: DataStream[String] = env.socketTextStream("node5", 9999)
    // 设置水位线
    val clickDSWithWatermark: DataStream[String] = clickDS.assignTimestampsAndWatermarks(
      // 设置watermark ,延迟时间为2s
      WatermarkStrategy
        .forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(element: String, recordTimestamp: Long): Long = {
            element.split(",")(2).toLong
          }
        })
    )

    // Interval Join
    loginDSWithWatermark.keyBy(loginInfo => loginInfo.split(",")(0))
      .intervalJoin(clickDSWithWatermark.keyBy(clickInfo => clickInfo.split(",")(0)))
      // 设置时间范围
      .between(Time.seconds(-2), Time.seconds(2))
      // 设置处理函数
      .process(new ProcessJoinFunction[String, String, String] {
        override def processElement(left: String,
                                    right: String,
                                    ctx: ProcessJoinFunction[String, String, String]#Context,
                                    out: Collector[String]): Unit = {
          // 获取用户ID
          val userId: String = left.split(",")(0)
          out.collect(s"用户ID为：$userId 的用户点击了广告：$right")
        }
      })
      .print()

    env.execute()
  }
}
