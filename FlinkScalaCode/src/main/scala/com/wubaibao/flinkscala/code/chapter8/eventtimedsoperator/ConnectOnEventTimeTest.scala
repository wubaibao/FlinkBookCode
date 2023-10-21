package com.wubaibao.flinkscala.code.chapter8.eventtimedsoperator

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * 案例：读取socket中数据流形成两个流，进行Connect关联观察水位线。
 */
object ConnectOnEventTimeTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //为了方便看出效果，这里设置并行度为1
    env.setParallelism(1)
    //设置隐式转换
    import org.apache.flink.streaming.api.scala._
    /**
     * 读取socket中数据流形成A流，并对A流设置watermark
     * 格式：001,181,182,busy,1000,10
     */
    val ADS: DataStream[String] = env.socketTextStream("node5", 8888)
      .map(line=>{
          val arr = line.split(",")
          //返回拼接字符串
          arr(0).trim() + "," +
            arr(1).trim() + "," +
            arr(2).trim() + "," +
            arr(3).trim() + "," +
            arr(4).trim() + "," +
            arr(5).trim()
      })

    //设置水位线
    val AdsWithWatermark: DataStream[String] = ADS.assignTimestampsAndWatermarks(
      //设置watermark ,延迟时间为2s
      WatermarkStrategy
        .forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        //设置时间戳列信息
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(str: String, recordTimestamp: Long): Long = {
            str.split(",")(4).toLong
          }
        })
    )

    /**
     * 读取socket中数据流形成B流,并对B流设置watermark
     * 格式：1,3000
     */
    val BDS: DataStream[String] = env.socketTextStream("node5", 9999)

    //设置水位线
    val BdsWithWatermark: DataStream[String] = BDS.assignTimestampsAndWatermarks(
      //设置watermark ,延迟时间为2s
      WatermarkStrategy
        .forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        //设置时间戳列信息
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(str: String, recordTimestamp: Long): Long = {
            str.split(",")(1).toLong
          }
        })
    )

    //两流进行connect操作
    AdsWithWatermark.connect(BdsWithWatermark)
      .process(new CoProcessFunction[String, String, String]() {
        override def processElement1(value: String,
                                     ctx: CoProcessFunction[String, String, String]#Context,
                                     out: Collector[String]): Unit = {
          out.collect("A流数据：" + value + ",当前watermark：" + ctx.timerService().currentWatermark())
        }

        override def processElement2(value: String,
                                     ctx: CoProcessFunction[String, String, String]#Context,
                                     out: Collector[String]): Unit = {
          out.collect("B流数据：" + value + ",当前watermark：" + ctx.timerService().currentWatermark())
        }
      }).print()

    env.execute()

  }

}
