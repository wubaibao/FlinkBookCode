package com.wubaibao.flinkscala.code.chapter9.tableoperator

import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, Table, TableDescriptor, TableEnvironment}

/**
 * Flink SQL 查询表数据
 * 案例：读取Kafka基站日志数据，统计每个基站通话总时长。
 * 要求：过滤通话成功并且通话时长大于10的数据信息。
 */
object QueryTableWithSQL {
  def main(args: Array[String]): Unit = {
    //创建TableEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()

    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    //读取Kafka基站日志数据，通过SQL DDL方式定义表结构
    tableEnv.executeSql(
      """
        |create table station_tbl(
        |   sid string,
        |   call_out string,
        |   call_in string,
        |   call_type string,
        |   call_time bigint,
        |   duration bigint
        |) with(
        |   'connector' = 'kafka',
        |   'topic' = 'stationlog-topic',
        |   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092',
        |   'properties.group.id' = 'test-group',
        |   'scan.startup.mode' = 'latest-offset',
        |   'format' = 'csv'
        |)
        |""".stripMargin)

    //通过SQL统计过滤通话成功并且通话时长大于10的数据信息
    val resultTbl: Table = tableEnv.sqlQuery(
      """
        |select sid,sum(duration) as total_duration
        |from station_tbl
        |where call_type = 'success' and duration > 10
        |group by sid
        |""".stripMargin)

    //打印输出
    resultTbl.execute().print()

  }

}
