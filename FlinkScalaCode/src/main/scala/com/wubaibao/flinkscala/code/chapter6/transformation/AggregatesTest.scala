package com.wubaibao.flinkscala.code.chapter6.transformation

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
 * Flink Aggregates 聚合算子测试
 */
object AggregatesTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 导入隐式转换
    import org.apache.flink.api.scala._

    val list: List[StationLog] = List(StationLog("sid1", "18600000000", "18600000001", "success", System.currentTimeMillis, 120L),
      StationLog("sid1", "18600000001", "18600000002", "fail", System.currentTimeMillis, 30L),
      StationLog("sid1", "18600000002", "18600000003", "busy", System.currentTimeMillis, 50L),
      StationLog("sid1", "18600000003", "18600000004", "barring", System.currentTimeMillis, 90L),
      StationLog("sid1", "18600000004", "18600000005", "success", System.currentTimeMillis, 300L))

    val ds: DataStream[StationLog] = env.fromCollection(list)
    val keyedStream: KeyedStream[StationLog, String] = ds.keyBy(stationLog => stationLog.sid)

    //统计duration 的总和
    keyedStream.sum("duration").print
    //统计duration的最小值，min返回该列最小值，其他列与第一条数据保持一致
    keyedStream.min("duration").print
    //统计duration的最小值，minBy返回的是最小值对应的整个对象
    keyedStream.minBy("duration").print
    //统计duration的最大值，max返回该列最大值，其他列与第一条数据保持一致
    keyedStream.max("duration").print
    //统计duration的最大值，maxBy返回的是最大值对应的整个对象
    keyedStream.maxBy("duration").print

    env.execute()

  }

}
