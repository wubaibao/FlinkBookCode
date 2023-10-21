package com.wubaibao.flinkscala.code.chapter8.eventtimedsoperator

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
 * Flink - WindowJoin
 * 案例：读取订单流和支付流，将订单流和支付流进行关联，输出关联后的数据
 */
object WindowJoinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //方便测试，并行度设置为1
    env.setParallelism(1)

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    /**
     * 读取socket中订单流，并对订单流设置watermark
     * 订单流数据格式:订单ID,用户ID,订单金额,时间戳
     * order1,user_1,10,1000
     * order2,user_2,20,2000
     * order3,user_3,30,3000
     */
    val orderDS: DataStream[String] = env.socketTextStream("node5", 8888)
    //设置水位线
    val orderDSWithWatermark: DataStream[String] = orderDS.assignTimestampsAndWatermarks(
      //设置watermark ,延迟时间为2s
      WatermarkStrategy
        .forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        //设置时间戳列信息
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(str: String, recordTimestamp: Long): Long =
            str.split(",")(3).toLong
        })
    )

    /**
     * 读取socket中支付流，并对支付流设置watermark
     * 支付流数据格式:订单ID,支付金额,时间戳
     * order1,10,1000
     * order2,20,2000
     * order3,30,3000
     */
    val payDS: DataStream[String] = env.socketTextStream("node5", 9999)
    //设置水位线
    val payDSWithWatermark: DataStream[String] = payDS.assignTimestampsAndWatermarks(
      //设置watermark ,延迟时间为2s
      WatermarkStrategy
        .forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        //设置时间戳列信息
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(str: String, recordTimestamp: Long): Long =
            str.split(",")(2).toLong
        })
    )

    //将订单流和支付流进行关联，并设置窗口
    val result: DataStream[String] = orderDSWithWatermark.join(payDSWithWatermark)
      //设置关联条件，where是订单流，equalTo是支付流
      .where(value=>value.split(",")(0))
      .equalTo(value=>value.split(",")(0))
      //设置窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //关联后的数据处理
      .apply(new JoinFunction[String, String, String] {
        override def join(orderInfo: String, payInfo: String): String =
          s"订单信息：$orderInfo - 支付信息：$payInfo"
      })

    result.print()

    env.execute()
  }

}
