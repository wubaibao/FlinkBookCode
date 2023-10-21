package com.wubaibao.flinkscala.code.chapter9.tableapi.windows

import com.wubaibao.flinkscala.code.chapter6.StationLog
import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Flink Table API  - Tumbling Window
 * 案例:读取socket基站日志数据，每隔5s统计窗口通话时长。
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    //获取DataStream的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //获取Taebl API的运行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //当某个并行度5秒没有数据输入时，自动推进watermark
    tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000")

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    val table: Table = tableEnv.fromDataStream(ds,
      Schema.newBuilder()
        //添加新列，第一个参数是新列名，第二个参数是表达式，这里是根据callTime字段转换为时间戳
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
        //指定字段的水位线,第一个参数是选取的事件时间字段，第二个参数是延迟时间
        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
        .build())

    val result: Table = table.window(Tumble over 5.second on $"rowtime" as "w")
      .groupBy($"sid", $"w")
      .select($"sid", $"w".start as "window_start", $"w".end as "window_end", $"duration".sum as "sum_duration")

    result.execute().print()

  }

}
