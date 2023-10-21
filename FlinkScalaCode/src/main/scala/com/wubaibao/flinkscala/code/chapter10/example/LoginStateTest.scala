package com.wubaibao.flinkscala.code.chapter10.example

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
 * Flink CEP 案例 - greedy测试，用户登录状态监测
 * 案例：读取用户登录日志，当用户登录成功后，输出用户登录成功前的所有操作
 */
case class LoginInfo(uid:String,userName:String,loginTime:Long,loginState:String)

object LoginStateTest {
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

    val pattern: Pattern[LoginInfo, LoginInfo] =
      Pattern.begin[LoginInfo]("first").where(_.loginState.startsWith("fail")).oneOrMore
      .followedBy("second").where(_.loginState.equals("success"))

    val patternStream: PatternStream[LoginInfo] = CEP.pattern(keyedStream, pattern)

    patternStream.process(new PatternProcessFunction[LoginInfo,String] {
      override def processMatch(`match`: util.Map[String, util.List[LoginInfo]],
                                ctx: PatternProcessFunction.Context,
                                out: Collector[String]): Unit = {
        val firstPatternInfo: util.List[LoginInfo] = `match`.get("first")
        val secondPatternInfo: util.List[LoginInfo] = `match`.get("second")

        //获取用户
        val uid: String = firstPatternInfo.get(0).uid

        //firstPatternInfo中登录状态拼接
        import scala.collection.JavaConverters._
        val end: String = firstPatternInfo.asScala.map(_.loginState).mkString(",")

        //获取成功登录的时间
        val loginTime: Long = secondPatternInfo.get(0).loginTime

        //输出
        out.collect(s"用户 $uid 在 $loginTime 登录成功，登录成功前的操作为：$end")

      }
    }).print()

    env.execute()


  }

}
