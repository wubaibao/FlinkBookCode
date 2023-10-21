package com.wubaibao.flinkscala.code.chapter7.keyedstate

import com.wubaibao.flinkscala.code.chapter6.StationLog
import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink MapState 状态编程
 * 案例：读取基站日志数据，统计主叫号码呼出的全部被叫号码及对应被叫号码通话总时长。
 */
object MapStateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

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
    val stationLogDS: DataStream[StationLog] = ds.map(new MapFunction[String, StationLog] {
      override def map(line: String): StationLog = {
        val arr: Array[String] = line.split(",")
        new StationLog(
          arr(0).trim,
          arr(1).trim,
          arr(2).trim,
          arr(3).trim,
          arr(4).toLong,
          arr(5).toLong
        )
      }
    })

    val result: DataStream[String] = stationLogDS.keyBy(_.callOut)
      .map(new RichMapFunction[StationLog, String] {
        //定义一个MapState 用来存放同一个主叫号码的所有被叫号码和对应的通话时长
        private var mapState: MapState[String, Long] = _

        override def open(parameters: Configuration): Unit = {
          //定义状态描述器
          val descriptor: MapStateDescriptor[String, Long] =
            new MapStateDescriptor[String, Long]("map-state", classOf[String], classOf[Long])
          //获取状态
          mapState = getRuntimeContext.getMapState(descriptor)
        }

        override def map(stationLog: StationLog): String = {
          val callIn: String = stationLog.callIn
          var duration: Long = stationLog.duration

          //从状态中获取当前主叫号码的被叫号码的通话时长，再设置回状态
          if (mapState.contains(callIn)) {
            duration += mapState.get(callIn)
            mapState.put(callIn, duration)
          } else {
            mapState.put(callIn, duration)
          }

          //遍历状态中的数据，将数据拼接成字符串返回
          var info: String = ""
          import scala.collection.JavaConverters._
          for (key <- mapState.keys().asScala) {
            info += "被叫：" + key + "，通话总时长：" + mapState.get(key) + "->"
          }
          info
        }
      })

    result.print()
    env.execute()
  }

}
