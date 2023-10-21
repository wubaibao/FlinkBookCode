package com.wubaibao.flinkscala.code.chapter6.processfun

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * Flink ProcessFunction 测试
 *  案例：Flink读取Socket中通话数据，如果被叫手机连续5s呼叫失败生成告警信息
 */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    //必须设置checkpoint，否则数据不能写入mysql
    env.enableCheckpointing(5000)

    /**
     * Socket中输入数据如下：
     * 001,186,187,fail,1000,10
     * 002,186,187,success,2000,20
     * 003,187,188,fail,3000,30
     * 004,187,188,fail,4000,40
     */
    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    //设置 被叫号码为key
    ds.keyBy(_.callIn).process(new KeyedProcessFunction[String,StationLog,String] {
      //定义一个状态，记录上次通话时间
      lazy val timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))


      //每条数据都会调用一次
      override def processElement(value: StationLog, ctx: KeyedProcessFunction[String, StationLog, String]#Context, out: Collector[String]): Unit = {
        //获取当前key对应的状态
        val time: Long = timeState.value()

        //如果该被叫手机号呼叫状态是fail且time为0，说明是第一条数据，注册定时器
        if("fail".equals(value.callType) && time == 0){
          //获取当前时间
          val nowTime: Long = ctx.timerService().currentProcessingTime()
          //触发定时器时间为当前时间+5s
          val onTime = nowTime + 5000

          //注册定时器
          ctx.timerService().registerProcessingTimeTimer(onTime)

          //更新定时器
          timeState.update(onTime)
        }

        //如果该被叫手机号呼叫状态不是fail且time不为0，表示有呼叫成功了，可以取消触发器
        if(!value.callType.equals("fail") &&  time!=0){
          //删除定时器
          ctx.timerService().deleteProcessingTimeTimer(time)
          //清空时间状态
          timeState.clear()
        }
      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
        //定时器触发时，说明该被叫手机号连续5s呼叫失败，输出告警信息
        out.collect("触发时间:" + timestamp + " 被叫手机号：" + ctx.getCurrentKey + " 连续5秒呼叫失败！")
        //清空时间状态
        timeState.clear()

      }
    }).print()
    env.execute()

  }

}
