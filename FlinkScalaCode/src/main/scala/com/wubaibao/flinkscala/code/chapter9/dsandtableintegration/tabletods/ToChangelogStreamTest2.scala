package com.wubaibao.flinkscala.code.chapter9.dsandtableintegration.tabletods

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Flink Table 转换为 DataStream
 * 使用 tableEnv.toChangelogStream(Table) 方法将 Table 转换为 DataStream
 */
object ToChangelogStreamTest2 {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.api.scala._

    //创建TableEnv
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //读取socket中基站日志数据并转换为StationgLog类型DataStream
    val stationLogDS: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    //将DataStream转换为Table
    val table: Table = tableEnv.fromDataStream(
      stationLogDS,
      Schema.newBuilder()
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
        .watermark("rowtime", "SOURCE_WATERMARK()")
        .build()
    )

    //使用Table API 对 Table进行查询
    val resultTable: Table = table
      .groupBy($"sid")
      .select($"sid", $"duration".sum().as("totalDuration"))

    //将Table转换为DataStream
    val rowDataStream: DataStream[Row] = tableEnv.toChangelogStream(
      resultTable,
      Schema.newBuilder()
        .column("sid", DataTypes.STRING())
        .column("totalDuration", DataTypes.BIGINT())
        .build(),
      ChangelogMode.upsert()
    )

    rowDataStream.print()

    env.execute()

  }

}

