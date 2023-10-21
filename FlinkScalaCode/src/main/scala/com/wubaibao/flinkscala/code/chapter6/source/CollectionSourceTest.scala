package com.wubaibao.flinkscala.code.chapter6.source

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Flink 读取集合中数据得到 DataStream
 */
object CollectionSourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val arrays: Array[StationLog] = Array(StationLog("001", "186", "187", "busy", 1000L, 0L),
      StationLog("002", "187", "186", "fail", 2000L, 0L),
      StationLog("003", "186", "188", "busy", 3000L, 0L),
      StationLog("004", "188", "186", "busy", 4000L, 0L),
      StationLog("005", "188", "187", "busy", 5000L, 0L))

    val dataStream: DataStream[StationLog] = env.fromCollection(arrays)
    dataStream.print()

    env.execute()
  }
}
