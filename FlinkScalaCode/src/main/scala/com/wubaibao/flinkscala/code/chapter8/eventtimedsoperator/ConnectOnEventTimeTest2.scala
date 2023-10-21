package com.wubaibao.flinkscala.code.chapter8.eventtimedsoperator

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * 案例：读取订单流和支付流，超过一定时间订单没有支付进行报警提示。
 */
object ConnectOnEventTimeTest2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //为了方便看出效果，这里设置并行度为1
    env.setParallelism(1)
    //设置隐式转换
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
          override def extractTimestamp(str: String, recordTimestamp: Long): Long = {
            str.split(",")(3).toLong
          }
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
          override def extractTimestamp(str: String, recordTimestamp: Long): Long = {
            str.split(",")(2).toLong
          }
        })
    )

    //两流进行connect操作
    orderDSWithWatermark.keyBy(_.split(",")(0))
      .connect(payDSWithWatermark.keyBy(_.split(",")(0)))
      .process(new KeyedCoProcessFunction[String,String,String,String] {
        //订单状态，存储订单信息
        var orderState:ValueState[String] = _

        //支付状态，存储支付信息
        var payState:ValueState[String] = _


        override def open(parameters: Configuration): Unit = {
          //定义两个状态，一个用来存放订单信息，一个用来存放支付信息
          val orderStateDescriptor: ValueStateDescriptor[String] = new ValueStateDescriptor[String]("order-state", classOf[String])
          val payStateDescriptor: ValueStateDescriptor[String] = new ValueStateDescriptor[String]("pay-state", classOf[String])

          orderState = getRuntimeContext.getState(orderStateDescriptor)
          payState = getRuntimeContext.getState(payStateDescriptor)
        }

        //处理订单流
        override def processElement1(orderInfo: String,
                                     ctx: KeyedCoProcessFunction[String, String, String, String]#Context,
                                     out: Collector[String]): Unit = {
          //当来一条订单数据后，判断支付状态是否为空，如果为空，说明订单没有支付，注册定时器，5秒后提示
          if (payState.value() == null) {
            //获取订单时间戳
            val orderTimestamp: Long = orderInfo.split(",")(3).toLong
            //注册定时器，设置定时器触发时间延后5s触发
            ctx.timerService().registerEventTimeTimer(orderTimestamp + 5*1000L)
            //更新订单状态
            orderState.update(orderInfo)
          }else{
            //如果支付状态不为空，说明订单已经支付，删除定时器
            ctx.timerService().deleteEventTimeTimer(payState.value().split(",")(2).toLong + 5*1000L)
            //清空订单状态
            payState.clear()
          }
        }

        //处理支付流
        override def processElement2(payInfo: String,
                                     ctx: KeyedCoProcessFunction[String, String, String, String]#Context,
                                     out: Collector[String]): Unit = {
          //当来一条支付数据后，判断订单状态是否为空，如果为空，说明订单没有支付，注册定时器，5秒后提示
          if (orderState.value() == null) {
            //注册定时器，设置定时器触发时间延后5s触发
            ctx.timerService().registerEventTimeTimer(payInfo.split(",")(2).toLong + 5*1000L)
            //更新支付状态
            payState.update(payInfo)
          }else{
            //如果订单状态不为空，说明订单已经支付，删除定时器
            ctx.timerService().deleteEventTimeTimer(orderState.value().split(",")(3).toLong + 5*1000L)
            //清空订单状态
            orderState.clear()
          }

       }

        //定时器触发后，执行的方法
        override def onTimer(timestamp: Long,
                             ctx: KeyedCoProcessFunction[String, String, String, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
          //判断订单状态是否为空，如果不为空，说明订单没有支付
          if (orderState.value() != null) {
            //输出提示信息
            out.collect("订单ID:" + orderState.value().split(",")(0) + "已经超过5s没有支付，请尽快支付！")
            //清空订单状态
            orderState.clear()
          }

          //判断支付状态是否为空，如果不为空，说明订单已经支付，但是没有订单信息！
          if (payState.value() != null) {
            //输出提示信息
            out.collect("订单ID:" + payState.value().split(",")(0) + "有异常，有支付信息没有订单信息！")
            //清空支付状态
            payState.clear()
          }

        }
      }).print()

    env.execute()
  }

}
