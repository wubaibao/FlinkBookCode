package com.wubaibao.flinkscala.code.chapter10

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
 * Flink CEP 测试
 * 案例：读取Socket基站日志数据，当同一个基站通话有3次失败时，输出报警信息
 */
object FlinkCepTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    //1.定义事件流
    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[StationLog](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(element: StationLog, recordTimestamp: Long): Long = element.callTime
        })
        .withIdleness(Duration.ofSeconds(5))
    )

    val keyedStream: KeyedStream[StationLog, String] = ds.keyBy(_.sid)

    //2.定义模式匹配
    val pattern: Pattern[StationLog, StationLog] = Pattern.begin[StationLog]("first")
      .where(_.callType.equals("fail"))
      .next("second")
      .where(_.callType.equals("fail"))
      .next("third")
      .where(_.callType.equals("fail"))

    //3.模式匹配
    val patternStream: PatternStream[StationLog] = CEP.pattern[StationLog](keyedStream, pattern)

    //4.获取符合规则的数据
    val result: DataStream[String] = patternStream.process(new PatternProcessFunction[StationLog, String] {
      override def processMatch(`match`: util.Map[String, util.List[StationLog]],
                                ctx: PatternProcessFunction.Context,
                                out: Collector[String]): Unit = {

        val first: StationLog = `match`.get("first").iterator().next()
        val second: StationLog = `match`.get("second").iterator().next()
        val third: StationLog = `match`.get("third").iterator().next()

        out.collect(s"预警信息：\n$first\n$second\n$third")

      }
    })

    result.print()
    env.execute()

  }

}
