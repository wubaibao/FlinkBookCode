package com.wubaibao.flinkscala.code.chapter9.udf

import java.lang.{Long => JLong, Integer => JInteger,Double => JDouble}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api.Expressions._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, Table, TableEnvironment, TableResult}

/**
 * Flink Table和SQL - 自定义聚合函数
 * 案例：读取Kafka数据形成表，通过自定义聚合函数统计每个基站平均通话时长
 */
object AggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    //创建TableEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()

    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    //当某个并行度5秒没有数据输入时，自动推进watermark
    tableEnv.getConfig.set("table.exec.source.idle-timeout", "5000")

    //读取Kafka基站日志数据，通过SQL DDL方式定义表结构
    tableEnv.executeSql("" +
      "create table stationlog_tbl (" +
      "   sid string," +
      "   call_out string," +
      "   call_in string," +
      "   call_type string," +
      "   call_time bigint," +
      "   duration bigint," +
      "   time_ltz AS TO_TIMESTAMP_LTZ(call_time,3)," +
      "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '2' SECOND" +
      ") with (" +
      "   'connector' = 'kafka'," +
      "   'topic' = 'stationlog-topic'," +
      "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id' = 'testGroup'," +
      "   'scan.startup.mode' = 'latest-offset'," +
      "   'format' = 'csv'" +
      ")")

    //注册自定义聚合函数
    tableEnv.createTemporarySystemFunction("my_avg", classOf[AvgDurationUDAF])

    //Table API 方式调用自定义表函数
//    val result1: Table = tableEnv.from("stationlog_tbl")
//      .groupBy($"sid")
//      .select($"sid",
//        call("my_avg", $"duration").as("avg_duration")
//      )
//
//    result1.execute().print()

    //SQL 方式调用自定义表函数
    val result2: Table = tableEnv.sqlQuery("" +
      "select sid,my_avg(duration) as avg_duration " +
      "from stationlog_tbl " +
      "group by sid"
    )

    result2.execute().print()

  }

}

class AvgDurationUDAF extends AggregateFunction[JDouble,JTuple2[JLong,JInteger]]{
  //初始化累加器
  override def createAccumulator(): JTuple2[JLong,JInteger] = JTuple2.of(0L,0)

  //累加器的计算逻辑
  def accumulate(acc: JTuple2[JLong,JInteger], duration: JLong): Unit = {
    acc.f0 = acc.f0 + duration
    acc.f1 = acc.f1 + 1
  }

  //返回结果
  override def getValue(acc: JTuple2[JLong,JInteger]): JDouble = {
    if(acc.f1 == 0){
      null
    }else{
      acc.f0  / acc.f1
    }
  }
}
