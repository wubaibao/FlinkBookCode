package com.wubaibao.flinkscala.code.chapter9.tableoperator

import org.apache.flink.table.api._

/**
 * Flink Table API & SQL 混合查询表数据
 * 案例：读取Kafka基站日志数据，统计每个基站通话总时长。
 * 要求：过滤通话成功并且通话时长大于10的数据信息。
 */
object QueryTableWithMix {
  def main(args: Array[String]): Unit = {
    //创建TableEnvironment
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()

    val tableEnv = TableEnvironment.create(settings)

    //读取Kafka基站日志数据，通过 TableDescriptor 定义表结构
    tableEnv.createTemporaryTable("station_tbl", TableDescriptor.forConnector("kafka")
      .schema(Schema.newBuilder()
        .column("sid", DataTypes.STRING())
        .column("call_out", DataTypes.STRING())
        .column("call_in", DataTypes.STRING())
        .column("call_type", DataTypes.STRING())
        .column("call_time", DataTypes.BIGINT())
        .column("duration", DataTypes.BIGINT())
        .build())
      .option("topic", "stationlog-topic")
      .option("properties.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("properties.group.id", "test-group")
      .option("scan.startup.mode", "latest-offset")
      .option("format", "csv")
      .build())

    //通过Table API 获取Table 对象，并过滤数据
    val filter: Table = tableEnv.from("station_tbl").filter($"call_type" === "success" && $"duration" > 10)

    //通过SQL统计通话数据信息
    val resultTbl: Table = tableEnv.sqlQuery("select sid,sum(duration) as total_duration from " + filter + " group by sid")

    //打印输出
    resultTbl.execute().print()
  }

}
