package com.wubaibao.flinkscala.code.chapter9.dsandtableintegration.tabletods

import com.wubaibao.flinkscala.code.chapter6.StationLog
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * Flink Table 转换为 DataStream
 * 使用 tableEnv.toChangelogStream(Table,AbstractDataType) 方法将 Table 转换为 DataStream
 */
object ToDataStreamTest2 {
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
    val table: Table = tableEnv.from("stationlog_tbl")

    //将Table转换为DataStream
    val resultDS: DataStream[StationLog] = tableEnv.toDataStream[StationLog](
      table,
      DataTypes.STRUCTURED(
        classOf[StationLog],
        DataTypes.FIELD("sid", DataTypes.STRING()),
        DataTypes.FIELD("callOut", DataTypes.STRING()),
        DataTypes.FIELD("callIn", DataTypes.STRING()),
        DataTypes.FIELD("callType", DataTypes.STRING()),
        DataTypes.FIELD("callTime", DataTypes.BIGINT()),
        DataTypes.FIELD("duration", DataTypes.BIGINT())
      )
    )

    //打印结果
    resultDS.print()

    env.execute()
  }

}
