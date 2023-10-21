package com.wubaibao.flinkscala.code.chapter9.udf

import java.lang.{Integer => JInteger, Long => JLong}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api.Expressions._
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, Table, TableEnvironment}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

/**
 * Flink Table和SQL - 自定义表聚合函数
 * 案例：读取Kafka数据形成表，通过自定义表聚合函数获取每个基站通话时长top2
 */
object TableAggregateFunctionTest {
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

    //注册自定义表聚合函数
    tableEnv.createTemporarySystemFunction("my_top2", classOf[Top2DurationTableUDAF])

    //Table API 方式调用自定义表聚合函数
    val result: Table = tableEnv.from("stationlog_tbl")
      .groupBy($"sid")
      .flatAggregate(call("my_top2", $"duration").as("top2_duration", "rank"))
      .select($"sid", $"top2_duration", $"rank")

    result.execute().print()
  }

}

/**
 * 自定义表聚合函数，获取每个基站通话时长top2
 * TableAggregateFunction<T,ACC>: T-最终聚合结果类型，ACC-累加器类型
 */
class Top2DurationTableUDAF extends TableAggregateFunction[JTuple2[JLong,JInteger],JTuple2[JLong,JLong]]{

  //创建累加器，累加器存储最大值和次大值
  override def createAccumulator(): JTuple2[JLong, JLong] = {
    JTuple2.of(JLong.MIN_VALUE,JLong.MIN_VALUE)
  }

  //累加器的计算逻辑：判断传入的duration是否大于累加器中的最大值或者次大值，如果大于则替换
  def accumulate(acc: JTuple2[JLong, JLong], duration: JLong): Unit = {
    if(duration > acc.f0){
      acc.f1 = acc.f0
      acc.f0 = duration
    }else if(duration > acc.f1){
      acc.f1 = duration
    }
  }

  //返回结果,将累加器中的最大值和次大值返回
  def emitValue(acc: JTuple2[JLong, JLong], out: Collector[JTuple2[JLong, JInteger]]): Unit = {
    if(acc.f0 != JLong.MIN_VALUE){
      out.collect(JTuple2.of(acc.f0,1))
    }
    if(acc.f1 != JLong.MIN_VALUE){
      out.collect(JTuple2.of(acc.f1,2))
    }
  }


}
