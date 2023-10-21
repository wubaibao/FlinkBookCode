package com.wubaibao.flinkscala.code.chapter10.example

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
 * Flink CEP - 恶意登录检测
 * 案例：读取用户登录日志，如果一个用户在20秒内连续三次登录失败，则是恶意登录，输出告警信息
 */
object LoginDetectTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val ds: DataStream[LoginInfo] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        LoginInfo(arr(0), arr(1), arr(2).toLong, arr(3))
      })

    val dsWithWatermark: DataStream[LoginInfo] = ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[LoginInfo](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[LoginInfo] {
          override def extractTimestamp(element: LoginInfo, recordTimestamp: Long): Long = element.loginTime
        })
        .withIdleness(Duration.ofSeconds(5))
    )

    val keyedStream: KeyedStream[LoginInfo, String] = dsWithWatermark.keyBy(_.uid)

    val pattern: Pattern[LoginInfo, LoginInfo] = Pattern.begin[LoginInfo]("first")
        .where(_.loginState.startsWith("fail"))
        .times(3)
        .within(Time.seconds(20))

    val patternStream: PatternStream[LoginInfo] = CEP.pattern(keyedStream, pattern)

    patternStream.process(new PatternProcessFunction[LoginInfo,String] {
      override def processMatch(`match`: util.Map[String, util.List[LoginInfo]],
                                ctx: PatternProcessFunction.Context,
                                out: Collector[String]): Unit = {
        val firstPatternInfo: util.List[LoginInfo] = `match`.get("first")

        //获取用户
        val uid: String = firstPatternInfo.get(0).uid

        //firstPatternInfo中登录状态拼接
        import scala.collection.JavaConverters._
        val end: String = firstPatternInfo.asScala.map(_.loginState).mkString(",")

        //输出
        out.collect("用户：" + uid + "在20秒内连续3次登录失败!")

      }
    }).print()

    env.execute()
  }

}
