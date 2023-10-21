package com.wubaibao.flinkscala.code.chapter7.broadcaststate

import com.wubaibao.flinkscala.code.chapter6.StationLog
import com.wubaibao.flinkscala.code.chapter7.PersonInfo
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * Flink BroadcastState 状态编程
 * 案例：读取两个Socket中的数据，一个是用户信息，一个是通话日志，
 * 将通话日志中的主叫号码和被叫号码与用户信息中的手机号码进行匹配，将用户信息补充到通话日志中。
 * 使用广播状态实现。
 */
object BroadcastStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    /**
     * 获取主流数据：通话日志
     * Socket 数据格式如下:
     * 001,181,182,busy,1000,10
     * 002,182,183,fail,2000,20
     * 003,183,184,busy,3000,30
     * 004,184,185,busy,4000,40
     * 005,181,183,busy,5000,50
     */
    val mainDS: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(new MapFunction[String, StationLog] {
        override def map(s: String): StationLog = {
          val arr = s.split(",")
          StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).toLong, arr(5).toLong)
        }
      })

    /**
     * 获取用户信息流：用户信息
     * Socket 数据格式如下:
     *  181,张三,北京
     *  182,李四,上海
     *  183,王五,广州
     *  184,赵六,深圳
     *  185,田七,杭州
     */
    val personInfoDS: DataStream[PersonInfo] = env.socketTextStream("node5", 8888)
      .map(new MapFunction[String, PersonInfo] {
        override def map(s: String): PersonInfo = {
          val arr = s.split(",")
          PersonInfo(arr(0).trim, arr(1).trim, arr(2).trim)
        }
      })

    // 定义广播状态描述器
    val mapStateDescriptor: MapStateDescriptor[String, PersonInfo] =
      new MapStateDescriptor[String, PersonInfo]("mapState", classOf[String], classOf[PersonInfo])

    // 将用户信息流广播出去
    val personInfoBroadcastStream: BroadcastStream[PersonInfo] = personInfoDS.broadcast(mapStateDescriptor)

    // 将主流和广播流进行连接
    val result: DataStream[String] = mainDS.connect(personInfoBroadcastStream)
      .process(new BroadcastProcessFunction[StationLog, PersonInfo, String] {
        // 处理主流数据
        override def processElement(stationLog: StationLog,
                                    ctx: BroadcastProcessFunction[StationLog, PersonInfo, String]#ReadOnlyContext,
                                    out: Collector[String]): Unit = {
          // 获取广播状态中的数据
          val callOutPersonInfo = ctx.getBroadcastState(mapStateDescriptor).get(stationLog.callOut)
          val callInPersonInfo = ctx.getBroadcastState(mapStateDescriptor).get(stationLog.callIn)
          val callOutName = Option(callOutPersonInfo).map(_.name).getOrElse("")
          val callOutCity = Option(callOutPersonInfo).map(_.city).getOrElse("")
          val callInName = Option(callInPersonInfo).map(_.name).getOrElse("")
          val callInCity = Option(callInPersonInfo).map(_.city).getOrElse("")

          // 输出数据
          out.collect("主叫姓名：" + callOutName + "，" +
            "主叫城市：" + callOutCity + "，" +
            "被叫姓名：" + callInName + "，" +
            "被叫城市：" + callInCity + "，" +
            "通话状态：" + stationLog.callType + "，" +
            "通话时长：" + stationLog.duration)
        }

        // 处理广播流数据
        override def processBroadcastElement(personInfo: PersonInfo, ctx: BroadcastProcessFunction[StationLog, PersonInfo, String]#Context, out: Collector[String]): Unit = {
          ctx.getBroadcastState(mapStateDescriptor).put(personInfo.phoneNum, personInfo)
        }
      })

    result.print()
    env.execute()
  }

}
