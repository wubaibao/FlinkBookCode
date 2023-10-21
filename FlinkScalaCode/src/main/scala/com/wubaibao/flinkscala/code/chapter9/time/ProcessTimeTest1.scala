package com.wubaibao.flinkscala.code.chapter9.time

import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}

/**
 * Flink Table API和SQL编程中指定 Processing Time
 * 案例:读取Kafka中基站日志数据，每隔5秒进行数据条数统计
 */
object ProcessTimeTest1 {
  def main(args: Array[String]): Unit = {
    //创建TableEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()

    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    //读取Kafka基站日志数据，通过SQL DDL方式定义表结构
    tableEnv.executeSql("" +
      "create table stationlog_tbl (" +
      "   sid string," +
      "   call_out string," +
      "   call_in string," +
      "   call_type string," +
      "   duration bigint," +
      "   call_time AS PROCTIME()" +
      ") with (" +
      "   'connector' = 'kafka'," +
      "   'topic' = 'stationlog-topic'," +
      "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id' = 'testGroup'," +
      "   'scan.startup.mode' = 'latest-offset'," +
      "   'format' = 'csv'" +
      ")")

    //通过SQL每隔5秒统计一次数据条数
    val result: Table = tableEnv.sqlQuery("" +
      "select " +
      "TUMBLE_START(call_time,INTERVAL '5' SECOND) as window_start," +
      "TUMBLE_END(call_time,INTERVAL '5' SECOND) as window_end," +
      "count(sid) as cnt " +
      "from stationlog_tbl " +
      "group by TUMBLE(call_time,INTERVAL '5' SECOND)")

    //打印结果
    result.execute().print()

  }

}
