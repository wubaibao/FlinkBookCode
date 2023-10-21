package com.wubaibao.flinkscala.code.chapter9.tableapi.overwindow

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._

/**
 * Flink Table API - 基于行计数间隔的开窗函数
 * 案例：读取基站日志数据，设置行计数间隔Over开窗函数，统计每个基站的通话时长
 */
object OverWindowBaseRowCount {
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

    //设置基于行计数间隔的Over开窗函数
    val window: OverWindowedTable = table.window(
      Over partitionBy $"sid" orderBy $"rowtime" preceding 2.rows following CURRENT_ROW as "w"
    )

    val result: Table = window.select(
      $"sid",
      $"duration",
      $"callTime",
      $"duration".sum over $"w" as "sum_duration"
    )

    //打印结果
    result.execute().print()
  }

}
