package com.wubaibao.flinkscala.code.chapter9

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, Table, TableDescriptor, TableEnvironment}
/**
 * Flink Table和SQL 代码测试
 */
object TableAndSQLTest {
  def main(args: Array[String]): Unit = {
    //1.准备环境配置
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()

    //2.创建TableEnvironment
    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    //设置并行度为1
    tableEnv.getConfig().getConfiguration().setString("parallelism.default", "1")

    //3.通过Table API 创建Source表
    tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
      .schema(Schema.newBuilder()
        .column("f0", DataTypes.STRING())
        .build())
      .option[java.lang.Long](DataGenConnectorOptions.ROWS_PER_SECOND, 10L)
      .build())

    //4.通过SQL DDL创建Sink Table
    tableEnv.executeSql("CREATE TABLE SinkTable (" +
      "  f0 STRING" +
      ") WITH (" +
      "  'connector' = 'print'" +
      ")")

    //5.通过Table API查询数据
    val table1: Table = tableEnv.from("SourceTable")

    //6.通过SQL查询数据
    val table2: Table = tableEnv.sqlQuery("select * from SourceTable")

    //7.通过Table API将查询结果写入SinkTable
    table1.executeInsert("SinkTable")

    //8.通过SQL将查询结果写入SinkTable
    tableEnv.executeSql("insert into SinkTable select * from SourceTable")

  }

}
