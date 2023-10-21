package com.wubaibao.flinkscala.code.chapter6.richfunction

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.text.SimpleDateFormat

/**
 * Flink 普通函数接口类测试
 */
object CommonFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.api.scala._

    /**
     * Socket中的数据格式如下:
     * 001,186,187,busy,1000,10
     * 002,187,186,fail,2000,20
     * 003,186,188,busy,3000,30
     * 004,188,186,busy,4000,40
     * 005,188,187,busy,5000,50
     */
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    ds.map(new MyMapFunction()).print()

    env.execute()
  }

  private class MyMapFunction extends MapFunction[String, String] {
    override def map(value: String): String = {
      //value格式：001,186,187,busy,1000,10
      val split: Array[String] = value.split(",")
      //获取通话时间，并转换成yyyy-MM-dd HH:mm:ss格式
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val startTime = sdf.format(split(4).toLong)

      //获取通话时长，通话时间加上通话时长，得到通话结束时间，转换成yyyy-MM-dd HH:mm:ss格式
      val duration = split(5)
      val endTime = sdf.format(split(4).toLong + duration.toLong)

      "基站ID:" + split(0) + ",主叫号码:" + split(1) + "," +
        "被叫号码:" + split(2) + ",通话类型:" + split(3) + "," +
        "通话开始时间:" + startTime + ",通话结束时间:" + endTime
    }
  }
}