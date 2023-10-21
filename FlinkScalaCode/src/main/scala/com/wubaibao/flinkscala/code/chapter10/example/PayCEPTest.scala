package com.wubaibao.flinkscala.code.chapter10.example

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.{PatternProcessFunction, TimedOutPartialMatchHandler}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
 * Flink CEP 实现订单支付超时监控
 * 案例：读取基站用户订单数据，实时监控用户订单是否支付超时，
 * 如果在20秒内没有支付则输出告警信息，
 * 如果在20秒内支付了则输出支付信息。
 */
case class OrderInfo(orderId: String, orderAmount: Double, orderTime: Long, payState:String)
object PayCEPTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val ds: DataStream[OrderInfo] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        OrderInfo(arr(0), arr(1).toDouble, arr(2).toLong, arr(3))
      })

    val dsWithWatermark: DataStream[OrderInfo] = ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[OrderInfo](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[OrderInfo] {
          override def extractTimestamp(element: OrderInfo, recordTimestamp: Long): Long = element.orderTime
        })
        .withIdleness(Duration.ofSeconds(5))
    )

    val keyedStream: KeyedStream[OrderInfo, String] = dsWithWatermark.keyBy(_.orderId)

    val pattern: Pattern[OrderInfo, OrderInfo] = Pattern.begin[OrderInfo]("first")
      .where(_.payState.equals("create"))
      .followedBy("second").where(_.payState.equals("pay")).within(Time.seconds(20))

    //定义outputTag
    val outputTag = new OutputTag[String]("pay-timeout")

    val patternStream: PatternStream[OrderInfo] = CEP.pattern(keyedStream, pattern)

    val result: DataStream[String] = patternStream.process(new MyPatternProcessFunction(outputTag))

    //打印结果
    result.print("订单支付：")

    //获取超时数据
    result.getSideOutput(outputTag).print("超时数据：")

    env.execute()

  }

}

class MyPatternProcessFunction(outputTag:OutputTag[String]) extends PatternProcessFunction[OrderInfo,String] with TimedOutPartialMatchHandler [OrderInfo]{

  //处理匹配到的数据
  override def processMatch(`match`: util.Map[String, util.List[OrderInfo]],
                            ctx: PatternProcessFunction.Context,
                            out: Collector[String]): Unit = {
    val firstPatternInfo: util.List[OrderInfo] = `match`.get("first")

    //获取订单
    val orderId = firstPatternInfo.get(0).orderId

    //输出
    out.collect("订单" + orderId + "支付成功,待发货")

  }

  //处理超时的数据
  override def processTimedOutMatch(`match`: util.Map[String, util.List[OrderInfo]],
                                    ctx: PatternProcessFunction.Context): Unit = {

    val firstPatternInfo: util.List[OrderInfo] = `match`.get("first")

    //获取订单
    val orderId = firstPatternInfo.get(0).orderId

    //输出到侧输出流
    ctx.output(outputTag,"订单" + orderId + "支付超时");
  }
}
