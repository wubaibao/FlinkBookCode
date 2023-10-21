package com.wubaibao.flinkscala.code.chapter7.keyedstate

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * Flink ValueState 状态编程
 * 案例: 读取基站日志数据，统计每个主叫手机通话间隔时间，单位为毫秒
 */
object ValueStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._

    /**
     * Socket中数据如下:
     * 001,186,187,busy,1000,10
     * 002,187,186,fail,2000,20
     * 003,186,188,busy,3000,30
     * 004,187,186,busy,4000,40
     * 005,189,187,busy,5000,50
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    //对ds进行转换处理，得到StationLog对象
    val stationLogDS: DataStream[StationLog] = ds.map(line => {
      val arr = line.split(",")
      StationLog(
        arr(0).trim,
        arr(1).trim,
        arr(2).trim,
        arr(3).trim,
        arr(4).toLong,
        arr(5).toLong
      )
    })

    val result: DataStream[String] = stationLogDS
      .keyBy(_.callOut)
      .process(new KeyedProcessFunction[String, StationLog, String] {
        //定义 ValueState，在Scala代码中默认值为0
        private var callTimeValueState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          //定义状态描述器
          val descriptor = new ValueStateDescriptor[Long]("callTimeValueState", classOf[Long])
          //获取状态
          callTimeValueState = getRuntimeContext.getState(descriptor)
        }

        override def processElement(stationLog: StationLog, ctx: KeyedProcessFunction[String, StationLog, String]#Context, out: Collector[String]): Unit = {
          //获取状态
          val callTime = callTimeValueState.value()
          println(s"状态中获取的状态值 = $callTime")
          if (callTime == 0) {
            //如果状态为空，说明是第一次通话，直接将当前通话时间存入状态
            callTimeValueState.update(stationLog.callTime)
          } else {
            //如果状态不为空，说明不是第一次通话，计算两次通话间隔时间
            val intervalTime = stationLog.callTime - callTime
            out.collect(s"主叫手机号为：${stationLog.callOut}，通话间隔时间为：$intervalTime")
            //更新状态
            callTimeValueState.update(stationLog.callTime)
          }
        }
      })

    result.print()
    env.execute()
  }

}
