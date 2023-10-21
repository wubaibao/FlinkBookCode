package com.wubaibao.flinkscala.code.chapter6.partitions

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * Flink Broadcast 算子测试
 */
object BroadCastTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._

    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    val mainDS: DataStream[(String, String)] = ds.map(one => {
      val arr: Array[String] = one.split(",")
      (arr(0), arr(1))
    })

    //获取用户基本信息
    val baseInfo: DataStream[(String, String)] = env.fromCollection(List(("zs", "北京"), ("ls", "上海"), ("ww", "广州")))

    //设置mapDescriptor
    val msd = new MapStateDescriptor[String, String]("map-descriptor", classOf[String], classOf[String])

    //广播用户基本信息
    val bcDS: BroadcastStream[(String, String)] = baseInfo.broadcast(msd)

    //连接主流和广播流
    mainDS.connect(bcDS).process(new BroadcastProcessFunction[(String,String),(String,String),String] {
      //处理主流数据
      override def processElement(value: (String, String), ctx: BroadcastProcessFunction[(String, String), (String, String), String]#ReadOnlyContext, out: Collector[String]): Unit = {
        //获取广播对象
        val map = ctx.getBroadcastState(msd)
        //获取学生地址信息
        val stuInfo = map.get(value._1)
        //输出学生信息
        out.collect(value._1 + "," + value._2 + "," + stuInfo)
      }

      //处理广播流数据
      override def processBroadcastElement(value: (String, String), ctx: BroadcastProcessFunction[(String, String), (String, String), String]#Context, out: Collector[String]): Unit = {
        //获取广播对象
        val map = ctx.getBroadcastState(msd)
        //将广播流数据放入广播对象中
        map.put(value._1,value._2)
      }
    }).print()
    env.execute()

  }

}
