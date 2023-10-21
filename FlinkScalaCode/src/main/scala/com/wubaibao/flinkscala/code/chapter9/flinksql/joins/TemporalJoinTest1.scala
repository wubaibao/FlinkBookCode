package com.wubaibao.flinkscala.code.chapter9.flinksql.joins

import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.functions.TemporalTableFunction

/**
 * Flink SQL - Join操作 - Temporal Join
 * 案例：读取Kafka中数据形成时态表，通过时态表函数查询时态表中数据。
 */
object TemporalJoinTest1 {
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

    //创建时态表函数，"right_time_ltz"为时间属性，"right_product_id"为主键
    val temporalTableFunction: TemporalTableFunction = tableEnv.from("product_tbl")
      .createTemporalTableFunction($("right_time_ltz"), $("right_product_id"))

    tableEnv.createTemporarySystemFunction("temporalTableFunction", temporalTableFunction)

    //SQL 方式实现 Temporal Join
    val result = tableEnv.sqlQuery("" +
      "select " +
      "  left_product_id,left_visit_time,right_product_name,right_price " +
      "from visit_tbl v,LATERAL TABLE (temporalTableFunction(left_time_ltz)) " +
      "WHERE  left_product_id = right_product_id"
    )

    tableEnv.sqlQuery("select * from product_tbl").execute().print()

    //打印结果
//    result.execute().print()

  }

}
