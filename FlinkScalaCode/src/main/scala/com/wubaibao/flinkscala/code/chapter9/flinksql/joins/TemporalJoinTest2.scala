package com.wubaibao.flinkscala.code.chapter9.flinksql.joins

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * Flink SQL - Join操作 - Temporal Join
 * 案例：案例：读取Kafka中数据形成时态表，通过”FOR SYSTEM_TIME AS OF “方式查询时态表中数据。
 */
object TemporalJoinTest2 {
  def main(args: Array[String]): Unit = {
    //创建TableEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()

    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    //当某个并行度5秒没有数据输入时，自动推进watermark
    tableEnv.getConfig.set("table.exec.source.idle-timeout", "5000")

    //读取Kafka 浏览商品数据，通过SQL DDL方式定义表结构
    tableEnv.executeSql("" +
      "create table visit_tbl (" +
      "   left_product_id string," +
      "   left_visit_time bigint," +
      "   left_time_ltz AS TO_TIMESTAMP_LTZ(left_visit_time,3)," +
      "   WATERMARK FOR left_time_ltz AS left_time_ltz - INTERVAL '5' SECOND" +
      ") with (" +
      "   'connector' = 'kafka'," +
      "   'topic' = 'visit-topic'," +
      "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id' = 'testGroup'," +
      "   'scan.startup.mode' = 'latest-offset'," +
      "   'format' = 'csv'" +
      ")")

    //读取Kafka 商品信息，通过SQL DDL方式定义表结构
    tableEnv.executeSql("" +
      "create table product_tbl (" +
      "   right_dt bigint," +
      "   right_product_id string," +
      "   right_product_name string," +
      "   right_price double," +
      "   PRIMARY KEY(right_product_id) NOT ENFORCED," +
      "   right_time_ltz AS TO_TIMESTAMP_LTZ(right_dt,3)," +
      "   WATERMARK FOR right_time_ltz AS right_time_ltz - INTERVAL '5' SECOND" +
      ") with (" +
      "   'connector' = 'kafka'," +
      "   'topic' = 'product-topic'," +
      "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id' = 'testGroup'," +
      "   'scan.startup.mode' = 'latest-offset'," +
      "   'format' = 'debezium-json'" +
      ")")

    //SQL 方式实现 Temporal Join
    val result = tableEnv.sqlQuery("" +
      "select " +
      "  left_product_id,left_visit_time,right_product_name,right_price " +
      "from visit_tbl " +
      "JOIN product_tbl FOR SYSTEM_TIME AS OF visit_tbl.left_time_ltz " +
      "ON left_product_id = right_product_id"
    );

    //打印结果
    result.execute().print()
  }

}
