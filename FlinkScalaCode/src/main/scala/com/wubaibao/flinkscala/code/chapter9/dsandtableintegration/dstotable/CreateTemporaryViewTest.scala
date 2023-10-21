package com.wubaibao.flinkscala.code.chapter9.dsandtableintegration.dstotable

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Flink DataStream 转换为 Table
 * 使用 tableEnv.createTemporaryView() 方法将 DataStream 转换为 Table
 */
object CreateTemporaryViewTest {
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
    tableEnv.createTemporaryView("stationlog_tbl", stationLogDS)

    //打印表结构
    val table: Table = tableEnv.from("stationlog_tbl")
    table.printSchema()

    //打印表数据
    table.execute().print()
  }

}
