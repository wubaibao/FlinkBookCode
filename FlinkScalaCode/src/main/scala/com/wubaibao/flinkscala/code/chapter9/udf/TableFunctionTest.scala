package com.wubaibao.flinkscala.code.chapter9.udf

import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.Expressions._
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, Table, TableEnvironment}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row


/**
 * Flink Table和SQL - 自定义表函数
 * 案例：读取Kafka数据形成表，通过自定义表函数对输入数据进行转换
 */
object TableFunctionTest {
  def main(args: Array[String]): Unit = {
    //创建TableEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()

    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    //当某个并行度5秒没有数据输入时，自动推进watermark
    tableEnv.getConfig.set("table.exec.source.idle-timeout", "5000")

    //读取Kafka 数据，通过SQL DDL方式定义表结构
    tableEnv.executeSql("" +
      "create table str_tbl (" +
      "   id string," +
      "   strs string," +
      "   dt bigint," +
      "   time_ltz AS TO_TIMESTAMP_LTZ(dt,3)," +
      "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '2' SECOND" +
      ") with (" +
      "   'connector' = 'kafka'," +
      "   'topic' = 'stationlog-topic'," +
      "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id' = 'testGroup'," +
      "   'scan.startup.mode' = 'latest-offset'," +
      "   'format' = 'csv'" +
      ")")

    //注册自定义表函数
    tableEnv.createTemporarySystemFunction("my_split", classOf[SplitStringUDTF])

    //Table API 方式调用自定义表函数
    /*val result1: Table = tableEnv.from("str_tbl")
      .joinLateral(call("my_split", $"strs").as("str", "len"))
      .select($"id", $"str", $"len")
    result1.execute().print()*/

    //SQL 方式调用自定义表函数
    val result2: Table = tableEnv.sqlQuery("" +
      "select " +
      "   id," +
      "   str," +
      "   len " +
      "from " +
      "   str_tbl," +
      "   lateral table(my_split(strs)) as T(str,len)")

    result2.execute().print()

  }

}


@FunctionHint(output = new DataTypeHint("ROW<word STRING, length INT>"))
class SplitStringUDTF extends TableFunction[Row] {
  def eval(str: String): Unit = {
    str.split("\\|").foreach(s => collect(Row.of(s, Int.box(s.length))))
  }
}
